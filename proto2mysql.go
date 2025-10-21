package proto2mysql

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/luyuancpp/proto2mysql/pbconv"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
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
	insertSQLTemplate            string   // 缓存INSERT模板
	deleteByPKTemplate           string   // 缓存按主键删除模板
	nullableFields               []string // 允许为NULL的字段

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
// getMySQLFieldType 获取字段对应的MySQL目标类型（支持Timestamp特殊处理）
func (m *MessageTable) getMySQLFieldType(fieldDesc protoreflect.FieldDescriptor) string {
	// 特殊处理Timestamp类型
	if fieldDesc.Message() != nil && fieldDesc.Message().FullName() == timestampFullName {
		baseType := "DATETIME NOT NULL"
		// 检查是否为 nullable 字段
		fieldName := string(fieldDesc.Name())
		for _, nullable := range m.nullableFields {
			if nullable == fieldName {
				baseType = "DATETIME"
				break
			}
		}
		return baseType
	}

	if fieldDesc.IsMap() || fieldDesc.IsList() {
		return "MEDIUMBLOB" // 集合类型统一用MEDIUMBLOB
	}

	fieldName := string(fieldDesc.Name())
	baseType, ok := MySQLFieldTypes[fieldDesc.Kind()]
	if !ok {
		baseType = "TEXT" // 默认类型
	}

	// 处理 nullable 字段
	for _, nullable := range m.nullableFields {
		if nullable == fieldName {
			baseType = strings.ReplaceAll(baseType, " NOT NULL", "")
			break
		}
	}

	// 处理自增字段：移除默认值（修复Error 1067）
	if m.autoIncreaseKey == fieldName {
		// 移除DEFAULT 0（避免自增字段默认值冲突）
		baseType = strings.ReplaceAll(baseType, " DEFAULT 0", "")
		baseType += " AUTO_INCREMENT"
	}

	return baseType
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

// CreateOrUpdateTable 尝试创建表，失败则自动更新表结构
func (p *PbMysqlDB) CreateOrUpdateTable(m proto.Message) error {
	tableName := GetTableName(m)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	// 先尝试创建表
	createSQL := table.GetCreateTableSQL()
	_, err := p.DB.Exec(createSQL)
	if err == nil {
		// 创建成功，更新缓存
		p.updateTableExistsCache(tableName, true)
		return nil
	}

	// 创建失败，判断是否需要更新表结构
	// 常见失败原因：表已存在、字段冲突等
	log.Printf("创建表 %s 失败: %v，尝试更新表结构...", tableName, err)

	// 强制更新表结构（无论表是否存在）
	return p.UpdateTableField(m)
}

// UpdateTableField 同步表字段
// UpdateTableField 同步表字段（增强版：无论表是否存在都尝试处理）
func (p *PbMysqlDB) UpdateTableField(m proto.Message) error {
	tableName := GetTableName(m)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	exists, err := p.IsTableExists(tableName)
	if err != nil {
		return fmt.Errorf("检查表 %s 存在性: %w", tableName, err)
	}

	// 如果表不存在，直接创建
	if !exists {
		createSQL := table.GetCreateTableSQL()
		if _, err := p.DB.Exec(createSQL); err != nil {
			return fmt.Errorf("创建表 %s 失败: %w, SQL: %s", tableName, err, createSQL)
		}
		p.updateTableExistsCache(tableName, true)
		return nil
	}

	// 表已存在，同步字段结构
	currentCols, err := p.getTableColumns(tableName)
	if err != nil {
		return fmt.Errorf("获取表 %s 字段: %w", tableName, err)
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
			// 字段存在但类型不兼容，修改字段类型
			if !isTypeMatch(currentType, targetType) {
				alterSQLs = append(alterSQLs, fmt.Sprintf("MODIFY COLUMN %s %s", escapeMySQLName(fieldName), targetType))
			}
			delete(currentCols, fieldName) // 标记为已处理
		} else {
			// 字段不存在，新增字段
			alterSQLs = append(alterSQLs, fmt.Sprintf("ADD COLUMN %s %s", escapeMySQLName(fieldName), targetType))
		}
	}

	// 执行ALTER TABLE（如果有需要修改的内容）
	if len(alterSQLs) > 0 {
		alterSQL := fmt.Sprintf("ALTER TABLE %s %s", escapeMySQLName(tableName), strings.Join(alterSQLs, ", "))
		_, err := p.DB.Exec(alterSQL)
		if err != nil {
			return fmt.Errorf("更新表 %s 结构失败: %w, SQL: %s", tableName, err, alterSQL)
		}
		p.clearColumnCache(tableName) // 清除缓存，下次查询时重新加载字段
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
		val, err := pbconv.SerializeFieldAsString(message, fieldDesc)
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
			val, err := pbconv.SerializeFieldAsString(msg, fieldDesc)
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
		val, err := pbconv.SerializeFieldAsString(message, fieldDesc)
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
	primaryKeyValue, err := pbconv.SerializeFieldAsString(message, m.primaryKeyField)
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
	val, err := pbconv.SerializeFieldAsString(message, m.primaryKeyField)
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
		val, err := pbconv.SerializeFieldAsString(message, fieldDesc)
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

		val, err := pbconv.SerializeFieldAsString(message, field)
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

		val, err := pbconv.SerializeFieldAsString(message, field)
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

		if err := pbconv.ParseFromString(message, result); err != nil {
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
		if err := pbconv.ParseFromString(listElement.Message().Interface(), result); err != nil {
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
// FindOneByWhereClause 非参数化的自定义条件查询（单条数据）
// 注意：whereClause 是纯条件字符串（如 "id = 100"，无需包含WHERE）
func (p *PbMysqlDB) FindOneByWhereClause(message proto.Message, whereClause string) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	// 内部自动拼接 WHERE 关键字（外部无需传入WHERE）
	whereSQL := "WHERE 1=1"
	if whereClause != "" {
		whereSQL = "WHERE " + whereClause
	}
	sqlStmt := table.GetSelectSQL(false) + " " + whereSQL + ";"

	rows, err := p.DB.Query(sqlStmt)
	if err != nil {
		return fmt.Errorf("exec select for table %s: %w, SQL: %s", tableName, err, sqlStmt)
	}
	defer rows.Close()

	// 处理结果集（省略重复代码，与之前一致）
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

		if err := pbconv.ParseFromString(message, result); err != nil {
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

// FindMultiByKV 通过键值对查询多条结果（简化版，自动拼接 "whereKey = ?" 条件）
// 注意：message 需是包含 repeated 字段的列表消息（如 golang_test_list）
func (p *PbMysqlDB) FindMultiByKV(message proto.Message, whereKey string, whereVal interface{}) error {
	// 直接复用 FindMultiByWhereWithArgs，自动拼接条件 "whereKey = ?"
	return p.FindMultiByWhereWithArgs(message, whereKey+" = ?", []interface{}{whereVal})
}

// FindMultiByWhereWithArgs 执行参数化的自定义WHERE查询（返回多条结果）
// 注意：message 需是一个包含 repeated 字段的“列表消息”（如 golang_test_list）
// 示例：传入 &golang_test_list{}，结果会填充到其 test_list 字段中
func (p *PbMysqlDB) FindMultiByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error {
	// 1. 解析列表消息中的 repeated 字段（用于存储多条结果）
	reflectionParent := message.ProtoReflect()
	md := reflectionParent.Descriptor()
	fieldDescriptors := md.Fields()

	var listField protoreflect.FieldDescriptor
	for i := 0; i < fieldDescriptors.Len(); i++ {
		fd := fieldDescriptors.Get(i)
		if fd.IsList() {
			if listField != nil {
				return ErrMultipleRepeated // 不允许多个repeated字段
			}
			listField = fd
		}
	}
	if listField == nil {
		return ErrNoRepeatedField // 必须包含repeated字段
	}

	// 2. 获取对应的表名（repeated字段的元素类型即表对应的消息类型）
	elementType := listField.Message() // repeated字段的元素类型（如 golang_test）
	tableName := string(elementType.Name())
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	// 3. 生成查询SQL并执行
	sqlWithArgs := table.GetSelectSQLByWhereWithArgs(whereClause, whereArgs)
	rows, err := p.DB.Query(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec multi select for table %s: %w, SQL: %s", tableName, err, sqlWithArgs.Sql)
	}
	defer rows.Close()

	// 4. 获取查询结果的列信息
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("get columns for table %s: %w", tableName, err)
	}

	// 5. 准备扫描结果的缓冲区
	columnValues := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k := range columnValues {
		scans[k] = &columnValues[k] // 绑定指针用于扫描
	}

	// 6. 清空列表并准备接收结果
	listValue := reflectionParent.Mutable(listField).List()
	listValue.Truncate(0) // 清空现有数据

	// 7. 遍历结果集并映射到消息
	for rows.Next() {
		// 扫描当前行数据
		if err := rows.Scan(scans...); err != nil {
			return fmt.Errorf("scan row for table %s: %w", tableName, err)
		}

		// 转换二进制结果为字符串数组
		resultRow := make([]string, len(columns))
		for i, v := range columnValues {
			resultRow[i] = string(v)
		}

		// 创建新的元素并解析数据
		listElement := listValue.NewElement()           // 创建repeated字段的元素实例
		elementMsg := listElement.Message().Interface() // 获取元素的消息接口
		if err := pbconv.ParseFromString(elementMsg, resultRow); err != nil {
			return fmt.Errorf("parse row for table %s: %w", tableName, err)
		}

		// 添加到列表
		listValue.Append(listElement)
	}

	// 8. 检查结果集遍历过程中的错误
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error for table %s: %w", tableName, err)
	}

	return nil
}

// FindMultiByWhereClause 非参数化的自定义条件查询（返回多条结果）
// 注意：1. message 需是包含 repeated 字段的列表消息（如 golang_test_list）
//  2. whereClause 是纯条件字符串（如 "player_id = 1000 AND group_id = 10"）
//  3. 不建议用于有用户输入的场景（有SQL注入风险），仅用于内部固定条件查询
//
// FindMultiByWhereClause 非参数化的自定义条件查询（返回多条结果）
// 注意：1. message 需是包含 repeated 字段的列表消息（如 golang_test_list）
//  2. whereClause 是纯条件字符串（如 "player_id = 1000 AND group_id = 10"）
//  3. 不建议用于有用户输入的场景（有SQL注入风险），仅用于内部固定条件查询
//
// FindMultiByWhereClause 非参数化的自定义条件查询（返回多条结果）
// 注意：1. message 需是包含 repeated 字段的列表消息（如 golang_test_list）
//  2. whereClause 是纯条件字符串（如 "player_id = 1000 AND group_id = 10"，无需包含WHERE）
//  3. 不建议用于有用户输入的场景（有SQL注入风险）
func (p *PbMysqlDB) FindMultiByWhereClause(message proto.Message, whereClause string) error {
	// 解析列表消息中的 repeated 字段
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

	// 获取表名（repeated字段的元素类型对应的表）
	elementType := listField.Message()
	tableName := string(elementType.Name())
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	// 内部自动拼接 WHERE 关键字（外部无需传入WHERE）
	// 若条件为空，默认查询所有数据（WHERE 1=1 兼容无条件场景）
	whereSQL := "WHERE 1=1"
	if whereClause != "" {
		whereSQL = "WHERE " + whereClause
	}
	sqlStmt := table.GetSelectSQL(false) + " " + whereSQL + ";"

	// 执行查询
	rows, err := p.DB.Query(sqlStmt)
	if err != nil {
		return fmt.Errorf("exec multi select (clause) for table %s: %w, SQL: %s", tableName, err, sqlStmt)
	}
	defer rows.Close()

	// 处理结果集（省略重复代码，与之前一致）
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

		resultRow := make([]string, len(columns))
		for i, v := range columnValues {
			resultRow[i] = string(v)
		}

		listElement := listValue.NewElement()
		elementMsg := listElement.Message().Interface()
		if err := pbconv.ParseFromString(elementMsg, resultRow); err != nil {
			return fmt.Errorf("parse row for table %s: %w", tableName, err)
		}
		listValue.Append(listElement)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error for table %s: %w", tableName, err)
	}

	return nil
}

// FindAllByWhereClause 兼容原有非参数化批量查询
// FindAllByWhereClause 兼容原有非参数化批量查询
// FindAllByWhereClause 兼容原有非参数化批量查询
// 注意：whereClause 是纯条件字符串（如 "id > 100"，无需包含WHERE）
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

	// 内部自动拼接 WHERE 关键字（外部无需传入WHERE）
	whereSQL := "WHERE 1=1"
	if whereClause != "" {
		whereSQL = "WHERE " + whereClause
	}
	sqlStmt := table.GetSelectSQL(false) + " " + whereSQL + ";"

	rows, err := p.DB.Query(sqlStmt)
	if err != nil {
		return fmt.Errorf("exec select all for table %s: %w, SQL: %s", tableName, err, sqlStmt)
	}
	defer rows.Close()

	// 处理结果集（省略重复代码，与之前一致）
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
		if err := pbconv.ParseFromString(listElement.Message().Interface(), result); err != nil {
			return fmt.Errorf("parse row for table %s: %w", tableName, err)
		}
		listValue.Append(listElement)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error for table %s: %w", tableName, err)
	}

	return nil
}

// MultiQuery 单张表的查询参数
type MultiQuery struct {
	Message     proto.Message // 用于接收查询结果的消息体
	WhereClause string        // 查询条件（如 "id = ?"）
	WhereArgs   []interface{} // 条件中的参数（与?对应）
}

// FindMultiByWhereClauses 一次查询多张无关表，返回多个结果
// FindMultiByWhereClauses 一次查询多张无关表，返回多个结果
func (p *PbMysqlDB) FindMultiByWhereClauses(queries []MultiQuery) error {
	if len(queries) == 0 {
		return errors.New("no queries provided")
	}

	// 1. 收集每张表的查询SQL（不含分号）
	var sqlParts []string
	var allArgs []interface{}
	for _, q := range queries {
		tableName := GetTableName(q.Message)
		table, ok := p.Tables[tableName]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
		}

		// 生成单表查询SQL（包含WHERE子句，不含分号）
		selectSQL := table.GetSelectSQL(false) + " WHERE " + q.WhereClause
		sqlParts = append(sqlParts, selectSQL)

		// 收集当前查询的参数
		allArgs = append(allArgs, q.WhereArgs...)
	}

	// 2. 用分号分隔多条SQL，最后不加多余分号（修复语法错误）
	sqlStmt := strings.Join(sqlParts, "; ")

	// 3. 执行批量查询
	rows, err := p.DB.Query(sqlStmt, allArgs...)
	if err != nil {
		return fmt.Errorf("exec multi select: %w, SQL: %s, args: %v", err, sqlStmt, allArgs)
	}
	defer rows.Close()

	// 4. 依次处理每个结果集（与queries顺序一致）
	for idx, q := range queries {
		tableName := GetTableName(q.Message)
		// 处理当前表的结果集
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
				return fmt.Errorf("%w: %s", ErrMultipleRowsFound, tableName)
			}
			if err := rows.Scan(scans...); err != nil {
				return fmt.Errorf("scan row for table %s: %w", tableName, err)
			}

			// 转换查询结果为字符串数组
			result := make([]string, len(columns))
			for i, v := range columnValues {
				result[i] = string(v)
			}

			// 映射到消息体
			if err := pbconv.ParseFromString(q.Message, result); err != nil {
				return fmt.Errorf("parse row for table %s: %w", tableName, err)
			}
			found = true
		}

		// 检查当前结果集的错误
		if err := rows.Err(); err != nil {
			return fmt.Errorf("rows error for table %s: %w", tableName, err)
		}
		if !found {
			return fmt.Errorf("%w: %s", ErrNoRowsFound, tableName)
		}

		// 切换到下一个结果集（最后一个不需要切换）
		if idx < len(queries)-1 {
			if !rows.NextResultSet() {
				return fmt.Errorf("missing result set for table %s", GetTableName(queries[idx+1].Message))
			}
		}
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

// WithNullableFields 设置允许为NULL的字段
func WithNullableFields(fields ...string) TableOption {
	return func(t *MessageTable) {
		t.nullableFields = fields
	}
}

// Close 关闭数据库连接
func (p *PbMysqlDB) Close() error {
	if p.DB == nil {
		return nil
	}
	return p.DB.Close()
}
