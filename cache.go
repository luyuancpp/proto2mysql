package proto2mysql

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
)

// ErrCacheMiss 缓存未命中时Cache.Get应返回的错误
var ErrCacheMiss = errors.New("cache miss")

// Cache 缓存抽象（cache-aside模式）。库不直接依赖具体Redis客户端，
// 由业务方注入实现（如go-redis适配器），保持弱依赖：
// 任何缓存错误都不会影响数据库操作，只会降级为直读DB。
//
// go-redis适配示例：
//
//	type RedisCache struct{ C *redis.Client }
//	func (r *RedisCache) Get(ctx context.Context, key string) ([]byte, error) {
//	    b, err := r.C.Get(ctx, key).Bytes()
//	    if errors.Is(err, redis.Nil) { return nil, proto2mysql.ErrCacheMiss }
//	    return b, err
//	}
//	func (r *RedisCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
//	    return r.C.Set(ctx, key, val, ttl).Err()
//	}
//	func (r *RedisCache) Del(ctx context.Context, keys ...string) error {
//	    return r.C.Del(ctx, keys...).Err()
//	}
type Cache interface {
	// Get 返回缓存值；未命中必须返回ErrCacheMiss
	Get(ctx context.Context, key string) ([]byte, error)
	// Set 写入缓存，ttl<=0表示不过期
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	// Del 删除一个或多个key
	Del(ctx context.Context, keys ...string) error
}

// EnableCache 启用cache-aside缓存（按主键的单行读写生效）。
// 语义：
//   - 读（FindOneByPK/FindOrCreate命中路径）：先查缓存，未命中读DB后回填；
//   - 写（Save/Update/UpdateFieldsByPK/UpdateKVByPK/UpdateIfVersion/Delete/IncrByPK等
//     按主键操作）：先写DB，成功后删缓存；
//   - 事务（RunInTransaction）：删缓存延迟到提交成功之后，避免回滚后缓存脏删；
//   - 降级：缓存Get/Set/Del出错仅记录日志，不影响DB结果（Redis弱依赖）。
//
// 注意：按WHERE条件的更新/删除（UpdateByWhereWithArgs/DeleteByWhereWithArgs/DeleteByKV）
// 无法定位受影响主键，不做缓存失效；缓存表请优先使用按主键的接口，
// 或调用InvalidateCache手动失效。
func (p *PbMysqlDB) EnableCache(cache Cache, ttl time.Duration) {
	p.cache = cache
	p.cacheTTL = ttl
}

// CacheKey 返回message对应的缓存key（pb:<表名>:<主键值...>），供业务方手动操作缓存
func (p *PbMysqlDB) CacheKey(message proto.Message) (string, error) {
	table, err := p.tableForMessage(message)
	if err != nil {
		return "", err
	}
	return cacheKeyFor(table, message)
}

// InvalidateCache 手动删除一批消息对应的缓存（按WHERE批量写后可调用）
func (p *PbMysqlDB) InvalidateCache(messages ...proto.Message) error {
	if !p.cacheEnabled() || len(messages) == 0 {
		return nil
	}

	keys := make([]string, 0, len(messages))
	for _, msg := range messages {
		key, err := p.CacheKey(msg)
		if err != nil {
			return err
		}
		keys = append(keys, key)
	}
	return p.cache.Del(context.Background(), keys...)
}

func (p *PbMysqlDB) cacheEnabled() bool {
	return p.cache != nil
}

// cacheKeyFor 生成缓存key：pb:<表名>:<主键值1>:<主键值2>...
func cacheKeyFor(table *MessageTable, message proto.Message) (string, error) {
	values, err := table.primaryKeyValues(message)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString("pb:")
	b.WriteString(table.tableName)
	for _, v := range values {
		b.WriteString(":")
		b.WriteString(fmt.Sprint(v))
	}
	return b.String(), nil
}

// cacheGetProto 读缓存并反序列化到message；返回是否命中。任何错误都视为未命中（降级）。
func (p *PbMysqlDB) cacheGetProto(table *MessageTable, message proto.Message) bool {
	key, err := cacheKeyFor(table, message)
	if err != nil {
		return false
	}

	data, err := p.cache.Get(context.Background(), key)
	if err != nil {
		if !errors.Is(err, ErrCacheMiss) {
			log.Printf("proto2mysql: cache get %s failed (fallback to db): %v", key, err)
		}
		return false
	}

	if err := proto.Unmarshal(data, message); err != nil {
		log.Printf("proto2mysql: cache unmarshal %s failed (fallback to db): %v", key, err)
		return false
	}
	return true
}

// cacheSetProto 序列化message并回填缓存（尽力而为，失败仅记日志）
func (p *PbMysqlDB) cacheSetProto(table *MessageTable, message proto.Message) {
	key, err := cacheKeyFor(table, message)
	if err != nil {
		return
	}

	data, err := proto.Marshal(message)
	if err != nil {
		log.Printf("proto2mysql: cache marshal %s failed: %v", key, err)
		return
	}
	if err := p.cache.Set(context.Background(), key, data, p.cacheTTL); err != nil {
		log.Printf("proto2mysql: cache set %s failed: %v", key, err)
	}
}

// cacheDelKeys 删除缓存key（尽力而为，失败仅记日志——存在短暂脏读风险，靠TTL兜底）
func (p *PbMysqlDB) cacheDelKeys(keys ...string) {
	if !p.cacheEnabled() || len(keys) == 0 {
		return
	}
	if err := p.cache.Del(context.Background(), keys...); err != nil {
		log.Printf("proto2mysql: cache del %v failed (stale until ttl): %v", keys, err)
	}
}

// invalidateMessages 写DB成功后失效缓存：
// 事务内先暂存key，提交成功后统一删除；非事务立即删除。
func (p *PbMysqlDB) invalidateMessages(table *MessageTable, messages ...proto.Message) {
	if !p.cacheEnabled() {
		return
	}

	keys := make([]string, 0, len(messages))
	for _, msg := range messages {
		key, err := cacheKeyFor(table, msg)
		if err != nil {
			continue // 无主键的表不参与缓存
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return
	}

	if p.tx != nil {
		p.pendingCacheDels = append(p.pendingCacheDels, keys...)
		return
	}
	p.cacheDelKeys(keys...)
}
