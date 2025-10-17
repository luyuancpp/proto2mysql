package protobuf_to_mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// 常量定义
const (
	PrimaryKeyIndex    = 0
	BatchInsertMaxSize = 1000 // 批量插入最大条数
	// MySQL关键字列表（增强版）
	mysqlKeywordPattern = `^(SELECT|INSERT|UPDATE|DELETE|FROM|WHERE|AND|OR|JOIN|ON|IN|NOT|NULL|PRIMARY|KEY|INDEX|UNIQUE|AUTO_INCREMENT|INT|VARCHAR|TEXT|BLOB|DATETIME|TIMESTAMP|FLOAT|DOUBLE|BOOL|TINYINT|BIGINT)$`
)

var (
	// 编译MySQL关键字正则
	keywordRegex = regexp.MustCompile(mysqlKeywordPattern)
	t            = timestamppb.Timestamp{}
	// 预定义Timestamp类型的全名
	timestampFullName = t.ProtoReflect().Descriptor().FullName()
	// 自定义错误类型
	ErrTableNotFound        = errors.New("table not found")
	ErrInvalidFieldKind     = errors.New("invalid field kind")
	ErrNoRepeatedField      = errors.New("message has no repeated field")
	ErrMultipleRepeated     = errors.New("message has multiple repeated fields")
	ErrPrimaryKeyNotFound   = errors.New("primary key not found")
	ErrFieldNotFound        = errors.New("field not found in message")
	ErrInvalidSQLTemplate   = errors.New("invalid SQL template")
	ErrTypeConversionFailed = errors.New("type conversion failed")
	ErrKeywordConflict      = errors.New("field name conflicts with MySQL keyword")
	ErrMultipleRowsFound    = errors.New("multiple rows found")
	ErrNoRowsFound          = errors.New("no rows found")
	ErrBatchSizeExceeded    = fmt.Errorf("batch size exceeds maximum %d", BatchInsertMaxSize)
)

// SqlWithArgs 存储带?占位符的SQL和对应的参数列表
type SqlWithArgs struct {
	Sql  string        // 带占位符的SQL
	Args []interface{} // 与占位符一一对应的参数值
}

// MessageTable 存储Protobuf消息与MySQL表的映射关系
type MessageTable struct {
	tableName                    string
	defaultInstance              proto.Message
	options                      protoreflect.Message
	primaryKeyField              protoreflect.FieldDescriptor
	autoIncrement                uint64
	fields                       map[int]string // 字段索引到名称的映射
	primaryKey                   []string       // 主键字段列表
	indexes                      []string       // 普通索引（逗号分隔字段）
	uniqueKeys                   string         // 唯一键（逗号分隔字段）
	autoIncreaseKey              string         // 自增字段名
	Descriptor                   protoreflect.MessageDescriptor
	selectAllSQLWithSemicolon    string
	selectAllSQLWithoutSemicolon string
	selectFieldsSQL              string
	fieldsListSQL                string
	replaceSQL                   string
	insertSQL                    string
	insertSQLTemplate            string // 缓存INSERT模板
	deleteByPKTemplate           string // 缓存按主键删除模板

	// 性能优化：缓存字段名到描述符的映射
	fieldNameToDesc map[string]protoreflect.FieldDescriptor
	// 表结构信息缓存（字段名到类型）
	cachedColumns map[string]string
	columnsMu     sync.RWMutex // 保护cachedColumns的并发安全
}

// SetAutoIncrement 设置自增起始值
func (m *MessageTable) SetAutoIncrement(autoIncrement uint64) {
	m.autoIncrement = autoIncrement
}

// DefaultInstance 获取默认消息实例
func (m *MessageTable) DefaultInstance() proto.Message {
	return m.defaultInstance
}

// getMySQLFieldType 获取字段对应的MySQL目标类型（支持Timestamp特殊处理）
func (m *MessageTable) getMySQLFieldType(fieldDesc protoreflect.FieldDescriptor) string {
	// 特殊处理Timestamp类型
	if fieldDesc.Message() != nil && fieldDesc.Message().FullName() == timestampFullName {
		return "DATETIME NOT NULL"
	}

	if fieldDesc.IsMap() || fieldDesc.IsList() {
		return "MEDIUMBLOB" // 集合类型统一用MEDIUMBLOB
	}
	if t, ok := MySQLFieldTypes[fieldDesc.Kind()]; ok {
		return t
	}
	return "TEXT" // 默认类型
}

// PbMysqlDB 管理所有表的数据库实例（移除事务相关）
type PbMysqlDB struct {
	Tables        map[string]*MessageTable
	DB            *sql.DB
	DBName        string
	sqlTemplateMu sync.RWMutex // 保护SQL模板的并发安全
	// 性能优化：缓存表是否存在的结果
	tableExistsCache map[string]bool
	tableExistsMu    sync.RWMutex
}

// OpenDB 打开数据库连接并切换数据库
func (p *PbMysqlDB) OpenDB(db *sql.DB, dbname string) error {
	p.DB = db
	p.DBName = dbname
	_, err := p.DB.Exec("USE " + p.DBName)
	return err
}

// 序列化字段（增强Timestamp处理）
func SerializeFieldAsString(message proto.Message, fieldDesc protoreflect.FieldDescriptor) (string, error) {
	reflection := message.ProtoReflect()

	// 特殊处理Timestamp类型
	if fieldDesc.Message() != nil && fieldDesc.Message().FullName() == timestampFullName {
		if !reflection.Has(fieldDesc) {
			return "", nil
		}
		ts := &timestamppb.Timestamp{}
		if err := proto.Unmarshal(reflection.Get(fieldDesc).Bytes(), ts); err != nil {
			return "", fmt.Errorf("serialize timestamp field %s: %w", fieldDesc.Name(), err)
		}
		return ts.AsTime().Format("2006-01-02 15:04:05"), nil
	}

	if fieldDesc.IsMap() {
		if !reflection.Has(fieldDesc) {
			return "", nil
		}
		mapWrapper := &MapWrapper{MapData: reflection.Get(fieldDesc).Map()}
		data, err := proto.Marshal(mapWrapper)
		if err != nil {
			return "", fmt.Errorf("serialize map field %s: %w", fieldDesc.Name(), err)
		}
		return string(data), nil
	}

	if fieldDesc.IsList() {
		if !reflection.Has(fieldDesc) {
			return "", nil
		}
		listWrapper := &ListWrapper{ListData: reflection.Get(fieldDesc).List()}
		data, err := proto.Marshal(listWrapper)
		if err != nil {
			return "", fmt.Errorf("serialize list field %s: %w", fieldDesc.Name(), err)
		}
		return string(data), nil
	}

	switch fieldDesc.Kind() {
	case protoreflect.Int32Kind:
		return fmt.Sprintf("%d", reflection.Get(fieldDesc).Int()), nil
	case protoreflect.Uint32Kind:
		return fmt.Sprintf("%d", reflection.Get(fieldDesc).Uint()), nil
	case protoreflect.FloatKind:
		return fmt.Sprintf("%g", reflection.Get(fieldDesc).Float()), nil
	case protoreflect.StringKind:
		return reflection.Get(fieldDesc).String(), nil
	case protoreflect.Int64Kind:
		return fmt.Sprintf("%d", reflection.Get(fieldDesc).Int()), nil
	case protoreflect.Uint64Kind:
		return fmt.Sprintf("%d", reflection.Get(fieldDesc).Uint()), nil
	case protoreflect.DoubleKind:
		return fmt.Sprintf("%g", reflection.Get(fieldDesc).Float()), nil
	case protoreflect.BoolKind:
		return fmt.Sprintf("%t", reflection.Get(fieldDesc).Bool()), nil
	case protoreflect.EnumKind:
		return fmt.Sprintf("%d", int32(reflection.Get(fieldDesc).Enum())), nil
	case protoreflect.BytesKind:
		return string(reflection.Get(fieldDesc).Bytes()), nil
	case protoreflect.MessageKind:
		if reflection.Has(fieldDesc) {
			subMsg := reflection.Get(fieldDesc).Message()
			data, err := proto.Marshal(subMsg.Interface())
			if err != nil {
				return "", fmt.Errorf("marshal sub-message field %s: %w", fieldDesc.Name(), err)
			}
			return string(data), nil
		}
		return "", nil
	default:
		return "", fmt.Errorf("%w: %v (field: %s)", ErrInvalidFieldKind, fieldDesc.Kind(), fieldDesc.Name())
	}
}

// 反序列化字段（增强Timestamp处理）
func ParseFromString(message proto.Message, row []string) error {
	reflection := message.ProtoReflect()
	desc := reflection.Descriptor()

	for i := 0; i < desc.Fields().Len(); i++ {
		if i >= len(row) {
			continue
		}

		fieldDesc := desc.Fields().Get(i)
		fieldValue := row[i]
		fieldName := fieldDesc.Name()

		// 特殊处理Timestamp类型
		if fieldDesc.Message() != nil && fieldDesc.Message().FullName() == timestampFullName {
			if fieldValue == "" {
				continue
			}
			t, err := time.Parse("2006-01-02 15:04:05", fieldValue)
			if err != nil {
				return fmt.Errorf("parse timestamp field %s: %w (value: %s)", fieldName, err, fieldValue)
			}
			ts := timestamppb.New(t)
			data, err := proto.Marshal(ts)
			if err != nil {
				return fmt.Errorf("marshal timestamp field %s: %w", fieldName, err)
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfBytes(data))
			continue
		}

		// 处理map类型
		if fieldDesc.IsMap() {
			if fieldValue == "" {
				continue
			}
			mapWrapper := &MapWrapper{}
			if err := proto.Unmarshal([]byte(fieldValue), mapWrapper); err != nil {
				return fmt.Errorf("parse map field %s: %w (value: %s)", fieldName, err, fieldValue)
			}
			mapVal := reflection.Mutable(fieldDesc).Map()
			mapWrapper.MapData.Range(func(key protoreflect.MapKey, val protoreflect.Value) bool {
				mapVal.Set(key, val)
				return true
			})
			continue
		}

		// 处理list类型
		if fieldDesc.IsList() {
			if fieldValue == "" {
				continue
			}
			listWrapper := &ListWrapper{}
			if err := proto.Unmarshal([]byte(fieldValue), listWrapper); err != nil {
				return fmt.Errorf("parse list field %s: %w (value: %s)", fieldName, err, fieldValue)
			}
			listVal := reflection.Mutable(fieldDesc).List()
			for i := 0; i < listWrapper.ListData.Len(); i++ {
				listVal.Append(listWrapper.ListData.Get(i))
			}
			continue
		}

		// 非集合类型处理
		if fieldValue == "" {
			switch fieldDesc.Kind() {
			case protoreflect.Int32Kind, protoreflect.Int64Kind:
				reflection.Set(fieldDesc, protoreflect.ValueOfInt64(0))
			case protoreflect.Uint32Kind, protoreflect.Uint64Kind:
				reflection.Set(fieldDesc, protoreflect.ValueOfUint64(0))
			case protoreflect.FloatKind, protoreflect.DoubleKind:
				reflection.Set(fieldDesc, protoreflect.ValueOfFloat64(0))
			case protoreflect.BoolKind:
				reflection.Set(fieldDesc, protoreflect.ValueOfBool(false))
			case protoreflect.StringKind:
				reflection.Set(fieldDesc, protoreflect.ValueOfString(""))
			}
			continue
		}

		// 解析非空值
		switch fieldDesc.Kind() {
		case protoreflect.Int32Kind:
			val, err := strconv.ParseInt(row[i], 10, 32)
			if err != nil {
				return fmt.Errorf("parse int32 field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfInt32(int32(val)))
		case protoreflect.Int64Kind:
			val, err := strconv.ParseInt(row[i], 10, 64)
			if err != nil {
				return fmt.Errorf("parse int64 field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfInt64(val))
		case protoreflect.Uint32Kind:
			val, err := strconv.ParseUint(row[i], 10, 32)
			if err != nil {
				return fmt.Errorf("parse uint32 field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfUint32(uint32(val)))
		case protoreflect.Uint64Kind:
			val, err := strconv.ParseUint(row[i], 10, 64)
			if err != nil {
				return fmt.Errorf("parse uint64 field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfUint64(val))
		case protoreflect.FloatKind:
			val, err := strconv.ParseFloat(row[i], 32)
			if err != nil {
				return fmt.Errorf("parse float field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfFloat32(float32(val)))
		case protoreflect.DoubleKind:
			val, err := strconv.ParseFloat(row[i], 64)
			if err != nil {
				return fmt.Errorf("parse double field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfFloat64(val))
		case protoreflect.StringKind:
			reflection.Set(fieldDesc, protoreflect.ValueOfString(row[i]))
		case protoreflect.BoolKind:
			val, err := strconv.ParseBool(row[i])
			if err != nil {
				return fmt.Errorf("parse bool field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfBool(val))
		case protoreflect.EnumKind:
			val, err := strconv.Atoi(row[i])
			if err != nil {
				return fmt.Errorf("parse enum field %s: %w (value: %s)", fieldName, err, row[i])
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfEnum(protoreflect.EnumNumber(val)))
		case protoreflect.BytesKind:
			reflection.Set(fieldDesc, protoreflect.ValueOfBytes([]byte(row[i])))
		case protoreflect.MessageKind:
			subMsg := reflection.Mutable(fieldDesc).Message()
			if err := proto.Unmarshal([]byte(row[i]), subMsg.Interface()); err != nil {
				return fmt.Errorf("unmarshal sub-message field %s: %w (value: %s)", fieldName, err, row[i])
			}
		default:
			return fmt.Errorf("%w: %v (field: %s)", ErrInvalidFieldKind, fieldDesc.Kind(), fieldName)
		}
	}

	return nil
}

// MapWrapper 临时包装器：用于序列化map
type MapWrapper struct {
	MapData protoreflect.Map
}

func (m *MapWrapper) ProtoReflect() protoreflect.Message {
	return protoreflect.Message(nil)
}

// ListWrapper 临时包装器：用于序列化list
type ListWrapper struct {
	ListData protoreflect.List
}

func (l *ListWrapper) ProtoReflect() protoreflect.Message {
	return protoreflect.Message(nil)
}

// MySQLFieldTypes MySQL字段类型映射表
var MySQLFieldTypes = map[protoreflect.Kind]string{
	protoreflect.Int32Kind:   "int NOT NULL DEFAULT 0",
	protoreflect.Uint32Kind:  "int unsigned NOT NULL DEFAULT 0",
	protoreflect.FloatKind:   "float NOT NULL DEFAULT 0",
	protoreflect.StringKind:  "MEDIUMTEXT",
	protoreflect.Int64Kind:   "bigint NOT NULL DEFAULT 0",
	protoreflect.Uint64Kind:  "bigint unsigned NOT NULL DEFAULT 0",
	protoreflect.DoubleKind:  "double NOT NULL DEFAULT 0",
	protoreflect.BoolKind:    "tinyint(1) NOT NULL DEFAULT 0",
	protoreflect.EnumKind:    "int NOT NULL DEFAULT 0",
	protoreflect.BytesKind:   "MEDIUMBLOB",
	protoreflect.MessageKind: "MEDIUMBLOB",
}

// 解析MySQL类型信息
type mysqlTypeInfo struct {
	baseType string
	length   int
	decimal  int
	unsigned bool
}

// 解析MySQL类型字符串
func parseMySQLType(colType string) mysqlTypeInfo {
	info := mysqlTypeInfo{}
	parts := strings.Fields(strings.ToLower(colType))

	if len(parts) == 0 {
		return info
	}

	// 提取基础类型
	basePart := parts[0]
	if idx := strings.Index(basePart, "("); idx != -1 {
		info.baseType = basePart[:idx]
		// 提取长度和小数位
		params := strings.Trim(basePart[idx:], "()")
		if strings.Contains(params, ",") {
			parts := strings.Split(params, ",")
			if len(parts) >= 1 {
				info.length, _ = strconv.Atoi(strings.TrimSpace(parts[0]))
			}
			if len(parts) >= 2 {
				info.decimal, _ = strconv.Atoi(strings.TrimSpace(parts[1]))
			}
		} else {
			info.length, _ = strconv.Atoi(params)
		}
	} else {
		info.baseType = basePart
	}

	// 检查是否为unsigned
	for _, part := range parts[1:] {
		if part == "unsigned" {
			info.unsigned = true
			break
		}
	}

	return info
}

// 判断两个MySQL类型是否匹配（增强版）
func isTypeMatch(currentType, targetType string) bool {
	current := parseMySQLType(currentType)
	target := parseMySQLType(targetType)

	// 基础类型映射表
	typeMap := map[string]string{
		"bool":       "tinyint",
		"integer":    "int",
		"mediumtext": "mediumtext",
		"text":       "text",
		"blob":       "blob",
		"mediumblob": "mediumblob",
		"datetime":   "datetime",
		"timestamp":  "datetime",
		"varchar":    "varchar",
		"char":       "char",
	}

	// 映射基础类型
	currentBase := typeMap[current.baseType]
	if currentBase == "" {
		currentBase = current.baseType
	}
	targetBase := typeMap[target.baseType]
	if targetBase == "" {
		targetBase = target.baseType
	}

	// 基础类型不匹配直接返回false
	if currentBase != targetBase {
		return false
	}

	// 特殊处理不同类型的长度兼容性
	switch currentBase {
	case "varchar", "char":
		// 目标长度大于等于当前长度视为兼容
		return target.length >= current.length
	case "int", "bigint", "tinyint", "smallint":
		// 无符号属性必须一致
		return current.unsigned == target.unsigned
	case "float", "double":
		// 小数位兼容检查
		return target.decimal >= current.decimal
	}

	return true
}

// escapeMySQLName 处理MySQL关键字冲突
func escapeMySQLName(name string) string {
	if keywordRegex.MatchString(strings.ToUpper(name)) {
		return fmt.Sprintf("`%s`", name)
	}
	return name
}

// GetCreateTableSQL 生成创建表的SQL语句
func (m *MessageTable) GetCreateTableSQL() string {
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", escapeMySQLName(m.tableName))
	fields := []string{}
	indexes := []string{}

	desc := m.Descriptor
	for i := 0; i < desc.Fields().Len(); i++ {
		field := desc.Fields().Get(i)
		fieldName := string(field.Name())
		escapedName := escapeMySQLName(fieldName)

		fieldType := m.getMySQLFieldType(field)

		if m.autoIncreaseKey == fieldName {
			fieldType += " AUTO_INCREMENT"
		}

		fields = append(fields, fmt.Sprintf("  %s %s", escapedName, fieldType))
	}

	if len(m.primaryKey) > 0 {
		primaryKeys := make([]string, len(m.primaryKey))
		for i, pk := range m.primaryKey {
			primaryKeys[i] = escapeMySQLName(pk)
		}
		fields = append(fields, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(primaryKeys, ",")))
	}

	if len(m.indexes) > 0 {
		for idx, indexCols := range m.indexes {
			cols := strings.Split(indexCols, ",")
			quotedCols := make([]string, len(cols))
			for i, col := range cols {
				quotedCols[i] = escapeMySQLName(col)
			}
			indexName := fmt.Sprintf("idx_%s_%d", m.tableName, idx)
			indexes = append(indexes, fmt.Sprintf("  INDEX %s (%s)", indexName, strings.Join(quotedCols, ",")))
		}
	}

	if m.uniqueKeys != "" {
		uniqueCols := strings.Split(m.uniqueKeys, ",")
		quotedUniqueCols := make([]string, len(uniqueCols))
		for i, col := range uniqueCols {
			quotedUniqueCols[i] = escapeMySQLName(col)
		}
		indexes = append(indexes, fmt.Sprintf("  UNIQUE KEY uk_%s (%s)", m.tableName, strings.Join(quotedUniqueCols, ",")))
	}

	stmt += strings.Join(fields, ",\n")
	if len(indexes) > 0 {
		stmt += ",\n" + strings.Join(indexes, ",\n")
	}

	// 表注释简化为表名
	stmt += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='" + escapeMySQLComment(m.tableName) + "';"
	return stmt
}

// escapeMySQLComment 转义MySQL注释中的特殊字符（仅保留基础转义）
func escapeMySQLComment(comment string) string {
	return strings.ReplaceAll(strings.ReplaceAll(comment, "'", "\\'"), "\n", " ")
}

// getTableColumns 获取表当前字段结构信息
func (p *PbMysqlDB) getTableColumns(tableName string) (map[string]string, error) {
	table, ok := p.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	table.columnsMu.RLock()
	if table.cachedColumns != nil {
		defer table.columnsMu.RUnlock()
		return table.cachedColumns, nil
	}
	table.columnsMu.RUnlock()

	query := `
		SELECT COLUMN_NAME, COLUMN_TYPE 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`
	rows, err := p.DB.Query(query, p.DBName, tableName)
	if err != nil {
		return nil, fmt.Errorf("query columns for table %s: %w", tableName, err)
	}
	defer rows.Close()

	columns := make(map[string]string)
	for rows.Next() {
		var colName, colType string
		if err := rows.Scan(&colName, &colType); err != nil {
			return nil, fmt.Errorf("scan columns for table %s: %w", tableName, err)
		}
		columns[colName] = colType
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error for table %s columns: %w", tableName, err)
	}

	table.columnsMu.Lock()
	table.cachedColumns = columns
	table.columnsMu.Unlock()

	return columns, nil
}

// clearColumnCache 清除表字段缓存
func (p *PbMysqlDB) clearColumnCache(tableName string) {
	if table, ok := p.Tables[tableName]; ok {
		table.columnsMu.Lock()
		table.cachedColumns = nil
		table.columnsMu.Unlock()
	}
}

// UpdateTableField 同步表字段
func (p *PbMysqlDB) UpdateTableField(m proto.Message) error {
	tableName := GetTableName(m)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	exists, err := p.IsTableExists(tableName)
	if err != nil {
		return fmt.Errorf("check table %s exists: %w", tableName, err)
	}
	if !exists {
		createSQL := table.GetCreateTableSQL()
		if _, err := p.DB.Exec(createSQL); err != nil {
			return fmt.Errorf("create table %s: %w, SQL: %s", tableName, err, createSQL)
		}
		p.updateTableExistsCache(tableName, true)
		return nil
	}

	currentCols, err := p.getTableColumns(tableName)
	if err != nil {
		return fmt.Errorf("get table %s columns: %w", tableName, err)
	}

	var alterSQLs []string
	desc := m.ProtoReflect().Descriptor()

	for i := 0; i < desc.Fields().Len(); i++ {
		fieldDesc := desc.Fields().Get(i)
		fieldName := string(fieldDesc.Name())

		if keywordRegex.MatchString(strings.ToUpper(fieldName)) {
			log.Printf("warning: field %s in table %s conflicts with MySQL keyword", fieldName, tableName)
		}

		targetType := table.getMySQLFieldType(fieldDesc)

		if currentType, exists := currentCols[fieldName]; exists {
			if !isTypeMatch(currentType, targetType) {
				alterSQLs = append(alterSQLs, fmt.Sprintf("MODIFY COLUMN %s %s", escapeMySQLName(fieldName), targetType))
			}
			delete(currentCols, fieldName)
		} else {
			alterSQLs = append(alterSQLs, fmt.Sprintf("ADD COLUMN %s %s", escapeMySQLName(fieldName), targetType))
		}
	}

	if len(alterSQLs) > 0 {
		alterSQL := fmt.Sprintf("ALTER TABLE %s %s", escapeMySQLName(tableName), strings.Join(alterSQLs, ", "))
		_, err := p.DB.Exec(alterSQL)
		if err != nil {
			return fmt.Errorf("exec ALTER for table %s: %w, SQL: %s", tableName, err, alterSQL)
		}
		p.clearColumnCache(tableName)
	}

	return nil
}

// IsTableExists 检查表是否存在
func (p *PbMysqlDB) IsTableExists(tableName string) (bool, error) {
	p.tableExistsMu.RLock()
	if exists, ok := p.tableExistsCache[tableName]; ok {
		p.tableExistsMu.RUnlock()
		return exists, nil
	}
	p.tableExistsMu.RUnlock()

	query := `
		SELECT COUNT(*) 
		FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`
	var count int
	err := p.DB.QueryRow(query, p.DBName, tableName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("query table %s exists: %w", tableName, err)
	}
	exists := count > 0

	p.tableExistsMu.Lock()
	p.tableExistsCache[tableName] = exists
	p.tableExistsMu.Unlock()

	return exists, nil
}

// updateTableExistsCache 更新表存在缓存
func (p *PbMysqlDB) updateTableExistsCache(tableName string, exists bool) {
	p.tableExistsMu.Lock()
	p.tableExistsCache[tableName] = exists
	p.tableExistsMu.Unlock()
}

// GetInsertSQLWithArgs 生成参数化的INSERT语句
func (m *MessageTable) GetInsertSQLWithArgs(message proto.Message) (*SqlWithArgs, error) {
	var args []interface{}
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		val, err := SerializeFieldAsString(message, fieldDesc)
		if err != nil {
			return nil, fmt.Errorf("serialize field %s: %w", fieldDesc.Name(), err)
		}
		args = append(args, val)
	}
	return &SqlWithArgs{Sql: m.insertSQLTemplate, Args: args}, nil
}

// GetBatchInsertSQLWithArgs 生成批量INSERT语句
func (m *MessageTable) GetBatchInsertSQLWithArgs(messages []proto.Message) (*SqlWithArgs, error) {
	if len(messages) == 0 {
		return nil, errors.New("no messages to insert")
	}
	if len(messages) > BatchInsertMaxSize {
		return nil, ErrBatchSizeExceeded
	}

	firstDesc := messages[0].ProtoReflect().Descriptor()
	for _, msg := range messages[1:] {
		if msg.ProtoReflect().Descriptor() != firstDesc {
			return nil, errors.New("messages have different descriptors")
		}
	}

	var allArgs []interface{}
	var valueGroups []string
	fieldCount := m.Descriptor.Fields().Len()

	for _, msg := range messages {
		args := make([]interface{}, 0, fieldCount)
		for i := 0; i < fieldCount; i++ {
			fieldDesc := m.Descriptor.Fields().Get(i)
			val, err := SerializeFieldAsString(msg, fieldDesc)
			if err != nil {
				return nil, fmt.Errorf("serialize field %s: %w", fieldDesc.Name(), err)
			}
			args = append(args, val)
		}
		allArgs = append(allArgs, args...)
		valueGroups = append(valueGroups, strings.Repeat("?, ", fieldCount)[:len(strings.Repeat("?, ", fieldCount))-2])
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		escapeMySQLName(m.tableName),
		m.fieldsListSQL,
		strings.Join(valueGroups, "), ("))
	return &SqlWithArgs{Sql: sql, Args: allArgs}, nil
}

// GetInsertOnDupUpdateSQLWithArgs 生成参数化的INSERT...ON DUPLICATE KEY UPDATE语句
func (m *MessageTable) GetInsertOnDupUpdateSQLWithArgs(message proto.Message) (*SqlWithArgs, error) {
	insertSQL, err := m.GetInsertSQLWithArgs(message)
	if insertSQL == nil || err != nil {
		return nil, err
	}

	var updateClauses []string
	var updateArgs []interface{}
	reflection := message.ProtoReflect()

	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		if !reflection.Has(fieldDesc) {
			continue
		}
		val, err := SerializeFieldAsString(message, fieldDesc)
		if err != nil {
			return nil, fmt.Errorf("serialize update field %s: %w", fieldDesc.Name(), err)
		}
		updateClauses = append(updateClauses, fmt.Sprintf("%s = ?", escapeMySQLName(string(fieldDesc.Name()))))
		updateArgs = append(updateArgs, val)
	}

	if len(updateClauses) == 0 {
		return insertSQL, nil
	}

	updateClauseStr := strings.Join(updateClauses, ", ")
	fullSQL := fmt.Sprintf("%s ON DUPLICATE KEY UPDATE %s", insertSQL.Sql, updateClauseStr)
	fullArgs := append(insertSQL.Args, updateArgs...)

	return &SqlWithArgs{Sql: fullSQL, Args: fullArgs}, nil
}

// GetInsertOnDupKeyForPrimaryKeyWithArgs 生成参数化的INSERT...更新主键语句
func (m *MessageTable) GetInsertOnDupKeyForPrimaryKeyWithArgs(message proto.Message) (*SqlWithArgs, error) {
	if m.primaryKeyField == nil {
		return nil, ErrPrimaryKeyNotFound
	}

	insertSQL, err := m.GetInsertSQLWithArgs(message)
	if insertSQL == nil || err != nil {
		return nil, err
	}

	primaryKeyName := string(m.primaryKeyField.Name())
	primaryKeyValue, err := SerializeFieldAsString(message, m.primaryKeyField)
	if err != nil {
		return nil, fmt.Errorf("serialize primary key: %w", err)
	}
	updateClause := fmt.Sprintf("%s = ?", escapeMySQLName(primaryKeyName))
	fullSQL := fmt.Sprintf("%s ON DUPLICATE KEY UPDATE %s", insertSQL.Sql, updateClause)
	fullArgs := append(insertSQL.Args, primaryKeyValue)

	return &SqlWithArgs{Sql: fullSQL, Args: fullArgs}, nil
}

// Insert 执行参数化的INSERT操作（直接用DB，无Tx）
func (p *PbMysqlDB) Insert(message proto.Message) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs, err := table.GetInsertSQLWithArgs(message)
	if sqlWithArgs == nil || err != nil {
		return fmt.Errorf("generate insert SQL for table %s: %w", tableName, err)
	}

	_, err = p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec insert for table %s: sql=%s, args=%v, err=%w",
			tableName, sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}
	return nil
}

// BatchInsert 执行批量INSERT操作（直接用DB，无Tx）
func (p *PbMysqlDB) BatchInsert(messages []proto.Message) error {
	if len(messages) == 0 {
		return errors.New("no messages to insert")
	}

	// 分批处理大批量数据
	for i := 0; i < len(messages); i += BatchInsertMaxSize {
		end := i + BatchInsertMaxSize
		if end > len(messages) {
			end = len(messages)
		}
		batch := messages[i:end]

		tableName := GetTableName(batch[0])
		table, ok := p.Tables[tableName]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
		}

		sqlWithArgs, err := table.GetBatchInsertSQLWithArgs(batch)
		if sqlWithArgs == nil || err != nil {
			return fmt.Errorf("generate batch insert SQL for table %s: %w", tableName, err)
		}

		_, err = p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
		if err != nil {
			return fmt.Errorf("exec batch insert for table %s: sql=%s, args len=%d, err=%w",
				tableName, sqlWithArgs.Sql, len(sqlWithArgs.Args), err)
		}
	}

	return nil
}

// InsertOnDupUpdate 执行参数化的INSERT...ON DUPLICATE KEY UPDATE操作（直接用DB，无Tx）
func (p *PbMysqlDB) InsertOnDupUpdate(message proto.Message) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs, err := table.GetInsertOnDupUpdateSQLWithArgs(message)
	if sqlWithArgs == nil || err != nil {
		return fmt.Errorf("generate insert on dup update SQL for table %s: %w", tableName, err)
	}

	_, err = p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec insert on dup update for table %s: sql=%s, args=%v, err=%w",
			tableName, sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}
	return nil
}

// GetSelectSQLByKVWithArgs 生成参数化的KV查询语句
func (m *MessageTable) GetSelectSQLByKVWithArgs(whereKey, whereVal string) (*SqlWithArgs, error) {
	if _, ok := m.fieldNameToDesc[whereKey]; !ok {
		return nil, fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, whereKey, m.tableName)
	}
	sql := fmt.Sprintf("%s WHERE %s = ?;", m.selectFieldsSQL, escapeMySQLName(whereKey))
	return &SqlWithArgs{Sql: sql, Args: []interface{}{whereVal}}, nil
}

// GetSelectSQLByWhereWithArgs 生成参数化的自定义WHERE查询语句
func (m *MessageTable) GetSelectSQLByWhereWithArgs(whereClause string, whereArgs []interface{}) *SqlWithArgs {
	sql := fmt.Sprintf("%s WHERE %s;", m.selectFieldsSQL, whereClause)
	return &SqlWithArgs{Sql: sql, Args: whereArgs}
}

// GetSelectSQL 合并版：生成查询语句
func (m *MessageTable) GetSelectSQL(includeSemicolon bool) string {
	if includeSemicolon {
		return m.selectAllSQLWithSemicolon
	}
	return m.selectAllSQLWithoutSemicolon
}

// GetDeleteSQLWithArgs 生成参数化的按主键删除语句
func (m *MessageTable) GetDeleteSQLWithArgs(message proto.Message) (*SqlWithArgs, error) {
	if m.primaryKeyField == nil {
		return nil, ErrPrimaryKeyNotFound
	}
	val, err := SerializeFieldAsString(message, m.primaryKeyField)
	if err != nil {
		return nil, fmt.Errorf("serialize primary key: %w", err)
	}
	return &SqlWithArgs{
		Sql:  m.deleteByPKTemplate,
		Args: []interface{}{val},
	}, nil
}

// GetDeleteSQLByWhereWithArgs 生成参数化的自定义WHERE删除语句
func (m *MessageTable) GetDeleteSQLByWhereWithArgs(whereClause string, whereArgs []interface{}) *SqlWithArgs {
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s", escapeMySQLName(m.tableName), whereClause)
	return &SqlWithArgs{Sql: sql, Args: whereArgs}
}

// Delete 执行参数化的按主键删除操作（直接用DB，无Tx）
func (p *PbMysqlDB) Delete(message proto.Message) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs, err := table.GetDeleteSQLWithArgs(message)
	if sqlWithArgs == nil || err != nil {
		return fmt.Errorf("generate delete SQL for table %s: %w", tableName, err)
	}

	_, err = p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec delete for table %s: sql=%s, args=%v, err=%w",
			tableName, sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}
	return nil
}

// GetReplaceSQLWithArgs 生成参数化的REPLACE语句
func (m *MessageTable) GetReplaceSQLWithArgs(message proto.Message) (*SqlWithArgs, error) {
	var args []interface{}
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		val, err := SerializeFieldAsString(message, fieldDesc)
		if err != nil {
			return nil, fmt.Errorf("serialize field %s: %w", fieldDesc.Name(), err)
		}
		args = append(args, val)
	}

	placeholders := strings.Repeat("?, ", len(args))
	placeholders = strings.TrimSuffix(placeholders, ", ")
	sqlStmt := fmt.Sprintf("%s%s)", m.replaceSQL, placeholders)

	return &SqlWithArgs{Sql: sqlStmt, Args: args}, nil
}

// GetUpdateSetWithArgs 生成参数化的SET子句和参数
func (m *MessageTable) GetUpdateSetWithArgs(message proto.Message) (setClause string, args []interface{}, err error) {
	needComma := false
	reflection := message.ProtoReflect()

	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		if !reflection.Has(field) {
			continue
		}

		val, err := SerializeFieldAsString(message, field)
		if err != nil {
			return "", nil, fmt.Errorf("serialize update field %s: %w", field.Name(), err)
		}

		if needComma {
			setClause += ", "
		} else {
			needComma = true
		}

		setClause += fmt.Sprintf(" %s = ?", escapeMySQLName(string(field.Name())))
		args = append(args, val)
	}

	return setClause, args, nil
}

// GetUpdateSQLWithArgs 生成参数化的按主键更新语句
func (m *MessageTable) GetUpdateSQLWithArgs(message proto.Message) (*SqlWithArgs, error) {
	setClause, setArgs, err := m.GetUpdateSetWithArgs(message)
	if err != nil {
		return nil, err
	}
	if setClause == "" {
		return nil, errors.New("no fields to update")
	}

	var whereClause string
	var whereArgs []interface{}
	needComma := false

	for _, primaryKey := range m.primaryKey {
		field, ok := m.fieldNameToDesc[primaryKey]
		if !ok {
			return nil, fmt.Errorf("%w: primary key %s in table %s", ErrFieldNotFound, primaryKey, m.tableName)
		}

		val, err := SerializeFieldAsString(message, field)
		if err != nil {
			return nil, fmt.Errorf("serialize primary key %s: %w", primaryKey, err)
		}

		if needComma {
			whereClause += " AND "
		} else {
			needComma = true
		}

		whereClause += fmt.Sprintf("%s = ?", escapeMySQLName(primaryKey))
		whereArgs = append(whereArgs, val)
	}

	if whereClause == "" {
		return nil, ErrPrimaryKeyNotFound
	}

	fullSQL := fmt.Sprintf("UPDATE %s SET %s WHERE %s", escapeMySQLName(m.tableName), setClause, whereClause)
	fullArgs := append(setArgs, whereArgs...)

	return &SqlWithArgs{Sql: fullSQL, Args: fullArgs}, nil
}

// GetUpdateSQLByWhereWithArgs 生成参数化的自定义WHERE更新语句
func (m *MessageTable) GetUpdateSQLByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) (*SqlWithArgs, error) {
	setClause, setArgs, err := m.GetUpdateSetWithArgs(message)
	if err != nil {
		return nil, err
	}
	if setClause == "" {
		return nil, errors.New("no fields to update")
	}

	fullSQL := fmt.Sprintf("UPDATE %s SET %s WHERE %s", escapeMySQLName(m.tableName), setClause, whereClause)
	fullArgs := append(setArgs, whereArgs...)

	return &SqlWithArgs{Sql: fullSQL, Args: fullArgs}, nil
}

// Update 执行参数化的按主键更新操作（直接用DB，无Tx）
func (p *PbMysqlDB) Update(message proto.Message) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs, err := table.GetUpdateSQLWithArgs(message)
	if sqlWithArgs == nil || err != nil {
		return fmt.Errorf("generate update SQL for table %s: %w", tableName, err)
	}

	_, err = p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec update for table %s: sql=%s, args=%v, err=%w",
			tableName, sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}
	return nil
}

// Init 初始化MessageTable的SQL片段
func (m *MessageTable) Init() {
	m.fieldNameToDesc = make(map[string]protoreflect.FieldDescriptor)

	needComma := false
	desc := m.Descriptor
	for i := 0; i < desc.Fields().Len(); i++ {
		field := desc.Fields().Get(i)
		fieldName := string(field.Name())
		m.fieldNameToDesc[fieldName] = field

		if needComma {
			m.fieldsListSQL += ", "
		} else {
			needComma = true
		}
		m.fieldsListSQL += escapeMySQLName(fieldName)
	}

	m.selectFieldsSQL = "SELECT " + m.fieldsListSQL + " FROM " + escapeMySQLName(m.tableName)
	m.selectAllSQLWithSemicolon = m.selectFieldsSQL + ";"
	m.selectAllSQLWithoutSemicolon = m.selectFieldsSQL + " "

	fieldCount := strings.Count(m.fieldsListSQL, ",") + 1
	placeholders := strings.Repeat("?, ", fieldCount)
	placeholders = strings.TrimSuffix(placeholders, ", ")
	m.insertSQLTemplate = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		escapeMySQLName(m.tableName),
		m.fieldsListSQL,
		placeholders)
	m.replaceSQL = "REPLACE INTO " + escapeMySQLName(m.tableName) + " (" + m.fieldsListSQL + ") VALUES ("
	m.insertSQL = "INSERT INTO " + escapeMySQLName(m.tableName) + " (" + m.fieldsListSQL + ") VALUES "

	if len(m.primaryKey) > 0 {
		m.primaryKeyField = m.Descriptor.Fields().ByName(protoreflect.Name(m.primaryKey[PrimaryKeyIndex]))
		if m.primaryKeyField != nil {
			m.deleteByPKTemplate = fmt.Sprintf("DELETE FROM %s WHERE %s = ?",
				escapeMySQLName(m.tableName),
				escapeMySQLName(string(m.primaryKeyField.Name())))
		}
	}
}

// NewPbMysqlDB 创建新的数据库实例
func NewPbMysqlDB() *PbMysqlDB {
	return &PbMysqlDB{
		Tables:           make(map[string]*MessageTable),
		tableExistsCache: make(map[string]bool),
	}
}

// GetTableName 获取Protobuf对应的表名
func GetTableName(m proto.Message) string {
	return string(m.ProtoReflect().Descriptor().FullName())
}

// GetDescriptor 获取Protobuf的消息描述符
func GetDescriptor(m proto.Message) protoreflect.MessageDescriptor {
	return m.ProtoReflect().Descriptor()
}

// GetCreateTableSQL 获取创建表的SQL（对外接口）
func (p *PbMysqlDB) GetCreateTableSQL(message proto.Message) string {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return ""
	}
	return table.GetCreateTableSQL()
}

// Save 执行参数化的REPLACE操作（直接用DB，无Tx）
func (p *PbMysqlDB) Save(message proto.Message) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs, err := table.GetReplaceSQLWithArgs(message)
	if sqlWithArgs == nil || err != nil {
		return fmt.Errorf("generate replace SQL for table %s: %w", tableName, err)
	}

	_, err = p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec replace for table %s: sql=%s, args=%v, err=%w",
			tableName, sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}
	return nil
}

// FindOneByKV 执行参数化的KV查询
func (p *PbMysqlDB) FindOneByKV(message proto.Message, whereKey string, whereVal string) error {
	return p.FindOneByWhereWithArgs(message, whereKey+" = ?", []interface{}{whereVal})
}

// FindOneByWhereWithArgs 执行参数化的自定义WHERE查询（单条数据）
func (p *PbMysqlDB) FindOneByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs := table.GetSelectSQLByWhereWithArgs(whereClause, whereArgs)
	rows, err := p.DB.Query(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec select for table %s: %w", tableName, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("get columns for table %s: %w", tableName, err)
	}

	columnValues := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k := range columnValues {
		scans[k] = &columnValues[k]
	}

	found := false
	for rows.Next() {
		if found {
			return ErrMultipleRowsFound
		}
		if err := rows.Scan(scans...); err != nil {
			return fmt.Errorf("scan row for table %s: %w", tableName, err)
		}

		result := make([]string, len(columns))
		for i, v := range columnValues {
			result[i] = string(v)
		}

		if err := ParseFromString(message, result); err != nil {
			return fmt.Errorf("parse row for table %s: %w", tableName, err)
		}
		found = true
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error for table %s: %w", tableName, err)
	}
	if !found {
		return ErrNoRowsFound
	}

	return nil
}

// FindAll 执行全表查询（批量数据）
func (p *PbMysqlDB) FindAll(message proto.Message) error {
	return p.FindAllByWhereWithArgs(message, "1=1", nil)
}

// FindAllByWhereWithArgs 执行参数化的自定义WHERE查询（批量数据）
func (p *PbMysqlDB) FindAllByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error {
	reflectionParent := message.ProtoReflect()
	md := reflectionParent.Descriptor()
	fieldDescriptors := md.Fields()

	var listField protoreflect.FieldDescriptor
	for i := 0; i < fieldDescriptors.Len(); i++ {
		fd := fieldDescriptors.Get(i)
		if fd.IsList() {
			if listField != nil {
				return ErrMultipleRepeated
			}
			listField = fd
		}
	}
	if listField == nil {
		return ErrNoRepeatedField
	}

	tableName := string(listField.Message().Name())
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs := table.GetSelectSQLByWhereWithArgs(whereClause, whereArgs)
	rows, err := p.DB.Query(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec select all for table %s: %w", tableName, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("get columns for table %s: %w", tableName, err)
	}

	columnValues := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k := range columnValues {
		scans[k] = &columnValues[k]
	}

	listValue := reflectionParent.Mutable(listField).List()
	listValue.Truncate(0)

	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return fmt.Errorf("scan row for table %s: %w", tableName, err)
		}

		result := make([]string, len(columns))
		for i, v := range columnValues {
			result[i] = string(v)
		}

		listElement := listValue.NewElement()
		if err := ParseFromString(listElement.Message().Interface(), result); err != nil {
			return fmt.Errorf("parse row for table %s: %w", tableName, err)
		}
		listValue.Append(listElement)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error for table %s: %w", tableName, err)
	}

	return nil
}

// FindOneByWhereClause 兼容原有非参数化查询
func (p *PbMysqlDB) FindOneByWhereClause(message proto.Message, whereClause string) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlStmt := table.GetSelectSQL(false) + whereClause + ";"
	rows, err := p.DB.Query(sqlStmt)
	if err != nil {
		return fmt.Errorf("exec select for table %s: %w, SQL: %s", tableName, err, sqlStmt)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("get columns for table %s: %w", tableName, err)
	}

	columnValues := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k := range columnValues {
		scans[k] = &columnValues[k]
	}

	found := false
	for rows.Next() {
		if found {
			return ErrMultipleRowsFound
		}
		if err := rows.Scan(scans...); err != nil {
			return fmt.Errorf("scan row for table %s: %w", tableName, err)
		}

		result := make([]string, len(columns))
		for i, v := range columnValues {
			result[i] = string(v)
		}

		if err := ParseFromString(message, result); err != nil {
			return fmt.Errorf("parse row for table %s: %w", tableName, err)
		}
		found = true
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error for table %s: %w", tableName, err)
	}
	if !found {
		return ErrNoRowsFound
	}

	return nil
}

// FindAllByWhereClause 兼容原有非参数化批量查询
func (p *PbMysqlDB) FindAllByWhereClause(message proto.Message, whereClause string) error {
	reflectionParent := message.ProtoReflect()
	md := reflectionParent.Descriptor()
	fieldDescriptors := md.Fields()

	var listField protoreflect.FieldDescriptor
	for i := 0; i < fieldDescriptors.Len(); i++ {
		fd := fieldDescriptors.Get(i)
		if fd.IsList() {
			if listField != nil {
				return ErrMultipleRepeated
			}
			listField = fd
		}
	}
	if listField == nil {
		return ErrNoRepeatedField
	}

	tableName := string(listField.Message().Name())
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlStmt := table.GetSelectSQL(false) + whereClause + ";"
	rows, err := p.DB.Query(sqlStmt)
	if err != nil {
		return fmt.Errorf("exec select all for table %s: %w, SQL: %s", tableName, err, sqlStmt)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("get columns for table %s: %w", tableName, err)
	}

	columnValues := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k := range columnValues {
		scans[k] = &columnValues[k]
	}

	listValue := reflectionParent.Mutable(listField).List()
	listValue.Truncate(0)

	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return fmt.Errorf("scan row for table %s: %w", tableName, err)
		}

		result := make([]string, len(columns))
		for i, v := range columnValues {
			result[i] = string(v)
		}

		listElement := listValue.NewElement()
		if err := ParseFromString(listElement.Message().Interface(), result); err != nil {
			return fmt.Errorf("parse row for table %s: %w", tableName, err)
		}
		listValue.Append(listElement)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error for table %s: %w", tableName, err)
	}

	return nil
}

// RegisterTable 注册Protobuf与表的映射关系
func (p *PbMysqlDB) RegisterTable(m proto.Message, opts ...TableOption) {
	tableName := GetTableName(m)
	table := &MessageTable{
		tableName:       tableName,
		defaultInstance: m,
		Descriptor:      GetDescriptor(m),
		options:         GetDescriptor(m).Options().ProtoReflect(),
		fields:          make(map[int]string),
	}

	for _, opt := range opts {
		opt(table)
	}

	p.Tables[tableName] = table
	table.Init()
}

// TableOption 表选项函数
type TableOption func(*MessageTable)

// WithPrimaryKey 设置主键
func WithPrimaryKey(keys ...string) TableOption {
	return func(t *MessageTable) {
		t.primaryKey = keys
	}
}

// WithIndexes 设置普通索引
func WithIndexes(indexes ...string) TableOption {
	return func(t *MessageTable) {
		t.indexes = indexes
	}
}

// WithUniqueKey 设置唯一键
func WithUniqueKey(uniqueKey string) TableOption {
	return func(t *MessageTable) {
		t.uniqueKeys = uniqueKey
	}
}

// WithAutoIncrementKey 设置自增字段
func WithAutoIncrementKey(key string) TableOption {
	return func(t *MessageTable) {
		t.autoIncreaseKey = key
	}
}

// Close 关闭数据库连接
func (p *PbMysqlDB) Close() error {
	if p.DB == nil {
		return nil
	}
	return p.DB.Close()
}
