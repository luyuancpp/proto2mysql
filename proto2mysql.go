package protobuf_to_mysql

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"log"
	"strconv"
	"strings"
)

// 常量：主键索引
const PrimaryKeyIndex = 0

// 自定义错误类型
var (
	ErrTableNotFound      = errors.New("table not found")
	ErrInvalidFieldKind   = errors.New("invalid field kind")
	ErrNoRepeatedField    = errors.New("message has no repeated field")
	ErrMultipleRepeated   = errors.New("message has multiple repeated fields")
	ErrPrimaryKeyNotFound = errors.New("primary key not found")
)

// SqlWithArgs 存储带?占位符的SQL和对应的参数列表
type SqlWithArgs struct {
	Sql  string        // 带占位符的SQL
	Args []interface{} // 与占位符一一对应的参数值
}

type MessageTable struct {
	tableName                    string
	defaultInstance              proto.Message
	options                      protoreflect.Message
	primaryKeyField              protoreflect.FieldDescriptor // 主键字段描述符
	autoIncrement                uint64
	fields                       map[int]string
	primaryKey                   []string
	indexes                      []string
	uniqueKeys                   string
	autoIncreaseKey              string
	Descriptor                   protoreflect.MessageDescriptor
	DB                           *sql.DB
	selectAllSQLWithSemicolon    string
	selectAllSQLWithoutSemicolon string

	selectFieldsSQL    string
	fieldsListSQL      string
	replaceSQL         string
	insertSQL          string
	insertSQLTemplate  string // 缓存INSERT模板
	deleteByPKTemplate string // 缓存按主键删除模板
}

func (m *MessageTable) SetAutoIncrement(autoIncrement uint64) {
	m.autoIncrement = autoIncrement
}

func (m *MessageTable) DefaultInstance() proto.Message {
	return m.defaultInstance
}

// 获取字段对应的MySQL目标类型
func (m *MessageTable) getMySQLFieldType(fieldDesc protoreflect.FieldDescriptor) string {
	if fieldDesc.IsMap() || fieldDesc.IsList() {
		return "MEDIUMBLOB" // 集合类型统一用MEDIUMBLOB
	}
	if t, ok := MySQLFieldTypes[fieldDesc.Kind()]; ok {
		return t
	}
	return "TEXT" // 默认类型
}

type PbMysqlDB struct {
	Tables map[string]*MessageTable
	DB     *sql.DB
	DBName string
}

func (p *PbMysqlDB) OpenDB(db *sql.DB, dbname string) error {
	p.DB = db
	p.DBName = dbname
	_, err := p.DB.Query("USE " + p.DBName)
	return err
}

// 序列化字段
func SerializeFieldAsString(message proto.Message, fieldDesc protoreflect.FieldDescriptor) string {
	reflection := message.ProtoReflect()

	if fieldDesc.IsMap() {
		if !reflection.Has(fieldDesc) {
			return ""
		}
		mapWrapper := &MapWrapper{MapData: reflection.Get(fieldDesc).Map()}
		data, err := proto.Marshal(mapWrapper)
		if err != nil {
			return ""
		}
		return string(data)
	}

	if fieldDesc.IsList() {
		if !reflection.Has(fieldDesc) {
			return ""
		}
		listWrapper := &ListWrapper{ListData: reflection.Get(fieldDesc).List()}
		data, err := proto.Marshal(listWrapper)
		if err != nil {
			return ""
		}
		return string(data)
	}

	switch fieldDesc.Kind() {
	case protoreflect.Int32Kind:
		return fmt.Sprintf("%d", reflection.Get(fieldDesc).Int())
	case protoreflect.Uint32Kind:
		return fmt.Sprintf("%d", reflection.Get(fieldDesc).Uint())
	case protoreflect.FloatKind:
		return fmt.Sprintf("%g", reflection.Get(fieldDesc).Float())
	case protoreflect.StringKind:
		return reflection.Get(fieldDesc).String()
	case protoreflect.Int64Kind:
		return fmt.Sprintf("%d", reflection.Get(fieldDesc).Int())
	case protoreflect.Uint64Kind:
		return fmt.Sprintf("%d", reflection.Get(fieldDesc).Uint())
	case protoreflect.DoubleKind:
		return fmt.Sprintf("%g", reflection.Get(fieldDesc).Float())
	case protoreflect.BoolKind:
		return fmt.Sprintf("%t", reflection.Get(fieldDesc).Bool())
	case protoreflect.EnumKind:
		return fmt.Sprintf("%d", int32(reflection.Get(fieldDesc).Enum()))
	case protoreflect.BytesKind:
		return string(reflection.Get(fieldDesc).Bytes())
	case protoreflect.MessageKind:
		if reflection.Has(fieldDesc) {
			subMsg := reflection.Get(fieldDesc).Message()
			data, err := proto.Marshal(subMsg.Interface())
			if err == nil {
				return string(data)
			}
			return "<marshal_error>"
		}
	}

	return ""
}

// 反序列化字段
func ParseFromString(message proto.Message, row []string) error {
	reflection := message.ProtoReflect()
	desc := reflection.Descriptor()

	for i := 0; i < desc.Fields().Len(); i++ {
		if i >= len(row) {
			continue
		}

		fieldDesc := desc.Fields().Get(i)
		fieldValue := row[i]

		// 处理map类型
		if fieldDesc.IsMap() {
			if fieldValue == "" {
				continue
			}
			mapWrapper := &MapWrapper{}
			if err := proto.Unmarshal([]byte(fieldValue), mapWrapper); err != nil {
				return fmt.Errorf("parse map (field: %s): %w", fieldDesc.Name(), err)
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
				return fmt.Errorf("parse list (field: %s): %w", fieldDesc.Name(), err)
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
				return fmt.Errorf("parse int32 failed: %w", err)
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfInt32(int32(val)))
		case protoreflect.Int64Kind:
			val, err := strconv.ParseInt(row[i], 10, 64)
			if err != nil {
				return fmt.Errorf("parse int64 failed: %w", err)
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfInt64(val))
		case protoreflect.Uint32Kind:
			val, err := strconv.ParseUint(row[i], 10, 32)
			if err != nil {
				return fmt.Errorf("parse uint32 failed: %w", err)
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfUint32(uint32(val)))
		case protoreflect.Uint64Kind:
			val, err := strconv.ParseUint(row[i], 10, 64)
			if err != nil {
				return fmt.Errorf("parse uint64 failed: %w", err)
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfUint64(val))
		case protoreflect.FloatKind:
			val, err := strconv.ParseFloat(row[i], 32)
			if err != nil {
				return fmt.Errorf("parse float failed: %w", err)
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfFloat32(float32(val)))
		case protoreflect.DoubleKind:
			val, err := strconv.ParseFloat(row[i], 64)
			if err != nil {
				return fmt.Errorf("parse double failed: %w", err)
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfFloat64(val))
		case protoreflect.StringKind:
			reflection.Set(fieldDesc, protoreflect.ValueOfString(row[i]))
		case protoreflect.BoolKind:
			val, err := strconv.ParseBool(row[i])
			if err != nil {
				return fmt.Errorf("parse bool failed: %w", err)
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfBool(val))
		case protoreflect.EnumKind:
			val, err := strconv.Atoi(row[i])
			if err != nil {
				return fmt.Errorf("parse enum failed: %w", err)
			}
			reflection.Set(fieldDesc, protoreflect.ValueOfEnum(protoreflect.EnumNumber(val)))
		case protoreflect.BytesKind:
			reflection.Set(fieldDesc, protoreflect.ValueOfBytes([]byte(row[i])))
		case protoreflect.MessageKind:
			subMsg := reflection.Mutable(fieldDesc).Message()
			if err := proto.Unmarshal([]byte(row[i]), subMsg.Interface()); err != nil {
				return fmt.Errorf("unmarshal sub-message failed: %w", err)
			}
		default:
			return fmt.Errorf("%w: %v", ErrInvalidFieldKind, fieldDesc.Kind())
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
	protoreflect.Int32Kind:   "int NOT NULL",
	protoreflect.Uint32Kind:  "int unsigned NOT NULL",
	protoreflect.FloatKind:   "float NOT NULL DEFAULT '0'",
	protoreflect.StringKind:  "TEXT", // 可改为MEDIUMTEXT等类型，会自动同步
	protoreflect.Int64Kind:   "bigint NOT NULL",
	protoreflect.Uint64Kind:  "bigint unsigned NOT NULL",
	protoreflect.DoubleKind:  "double NOT NULL DEFAULT '0'",
	protoreflect.BoolKind:    "bool",
	protoreflect.EnumKind:    "int NOT NULL",
	protoreflect.BytesKind:   "MEDIUMBLOB",
	protoreflect.MessageKind: "MEDIUMBLOB",
}

// 清理MySQL类型字符串（移除修饰符）
func cleanMySQLType(colType string) string {
	parts := strings.Fields(strings.ToLower(colType))
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}

// 判断两个MySQL类型是否匹配
func isTypeMatch(currentType, targetType string) bool {
	cleanCurrent := cleanMySQLType(currentType)
	cleanTarget := cleanMySQLType(targetType)

	typeMap := map[string]string{
		"bool":       "tinyint",
		"mediumtext": "mediumtext",
		"text":       "text",
		"blob":       "blob",
		"mediumblob": "mediumblob",
	}

	current := typeMap[cleanCurrent]
	if current == "" {
		current = cleanCurrent
	}
	target := typeMap[cleanTarget]
	if target == "" {
		target = cleanTarget
	}
	return current == target
}

// GetCreateTableSQL 生成创建表的SQL语句
func (m *MessageTable) GetCreateTableSQL() string {
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", m.tableName)
	fields := []string{}
	indexes := []string{}

	// 1. 解析字段定义
	desc := m.Descriptor
	for i := 0; i < desc.Fields().Len(); i++ {
		field := desc.Fields().Get(i)
		fieldName := string(field.Name())

		// 获取MySQL字段类型
		fieldType := m.getMySQLFieldType(field)

		// 处理自增字段
		if m.autoIncreaseKey == fieldName {
			fieldType += " AUTO_INCREMENT"
		}

		fields = append(fields, fmt.Sprintf("  `%s` %s", fieldName, fieldType))
	}

	// 2. 处理主键
	if len(m.primaryKey) > 0 {
		primaryKeys := make([]string, len(m.primaryKey))
		for i, pk := range m.primaryKey {
			primaryKeys[i] = fmt.Sprintf("`%s`", pk)
		}
		fields = append(fields, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(primaryKeys, ",")))
	}

	// 3. 处理普通索引
	if len(m.indexes) > 0 {
		for idx, indexCols := range m.indexes {
			cols := strings.Split(indexCols, ",")
			quotedCols := make([]string, len(cols))
			for i, col := range cols {
				quotedCols[i] = fmt.Sprintf("`%s`", col)
			}
			indexes = append(indexes, fmt.Sprintf("  INDEX idx_%d (%s)", idx, strings.Join(quotedCols, ",")))
		}
	}

	// 4. 处理唯一索引
	if m.uniqueKeys != "" {
		uniqueCols := strings.Split(m.uniqueKeys, ",")
		quotedUniqueCols := make([]string, len(uniqueCols))
		for i, col := range uniqueCols {
			quotedUniqueCols[i] = fmt.Sprintf("`%s`", col)
		}
		indexes = append(indexes, fmt.Sprintf("  UNIQUE KEY uk (%s)", strings.Join(quotedUniqueCols, ",")))
	}

	// 5. 组合语句
	stmt += strings.Join(fields, ",\n")
	if len(indexes) > 0 {
		stmt += ",\n" + strings.Join(indexes, ",\n")
	}

	// 6. 表选项（字符集和引擎）
	stmt += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
	return stmt
}

// 获取表当前字段结构信息
func (p *PbMysqlDB) getTableColumns(tableName string) (map[string]string, error) {
	query := `
		SELECT COLUMN_NAME, COLUMN_TYPE 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`
	rows, err := p.DB.Query(query, p.DBName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string]string)
	for rows.Next() {
		var colName, colType string
		if err := rows.Scan(&colName, &colType); err != nil {
			return nil, err
		}
		columns[colName] = colType
	}
	return columns, nil
}

// UpdateTableField 同步表字段（新增字段+修改类型不匹配的字段）
func (p *PbMysqlDB) UpdateTableField(m proto.Message) error {
	tableName := GetTableName(m)
	table, ok := p.Tables[tableName]
	if !ok {
		return ErrTableNotFound
	}

	// 1. 获取当前表的字段结构
	currentCols, err := p.getTableColumns(tableName)
	if err != nil {
		return fmt.Errorf("获取表结构失败: %w", err)
	}

	// 2. 遍历Protobuf字段，计算需要修改的字段
	var alterSQLs []string
	desc := m.ProtoReflect().Descriptor()

	for i := 0; i < desc.Fields().Len(); i++ {
		fieldDesc := desc.Fields().Get(i)
		fieldName := string(fieldDesc.Name())

		// 获取目标类型（来自MySQLFieldTypes映射）
		targetType := table.getMySQLFieldType(fieldDesc)

		// 字段已存在，检查类型是否匹配
		if currentType, exists := currentCols[fieldName]; exists {
			if !isTypeMatch(currentType, targetType) {
				// 类型不匹配，添加修改语句
				alterSQLs = append(alterSQLs, fmt.Sprintf("MODIFY COLUMN `%s` %s", fieldName, targetType))
			}
			delete(currentCols, fieldName) // 标记为已处理
		} else {
			// 字段不存在，添加新增语句
			alterSQLs = append(alterSQLs, fmt.Sprintf("ADD COLUMN `%s` %s", fieldName, targetType))
		}
	}

	// 3. 执行ALTER语句（如果有需要修改的内容）
	if len(alterSQLs) > 0 {
		alterSQL := fmt.Sprintf("ALTER TABLE `%s` %s", tableName, strings.Join(alterSQLs, ", "))
		_, err := p.DB.Exec(alterSQL)
		if err != nil {
			return fmt.Errorf("执行ALTER语句失败: %w, SQL: %s", err, alterSQL)
		}
	}

	return nil
}

// GetInsertSQLWithArgs 生成参数化的INSERT语句
func (m *MessageTable) GetInsertSQLWithArgs(message proto.Message) *SqlWithArgs {
	var args []interface{}
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		args = append(args, SerializeFieldAsString(message, fieldDesc))
	}
	return &SqlWithArgs{Sql: m.insertSQLTemplate, Args: args}
}

// GetInsertOnDupUpdateSQLWithArgs 生成参数化的INSERT...ON DUPLICATE KEY UPDATE语句
func (m *MessageTable) GetInsertOnDupUpdateSQLWithArgs(message proto.Message) *SqlWithArgs {
	insertSQL := m.GetInsertSQLWithArgs(message)
	if insertSQL == nil {
		return nil
	}

	var updateClauses []string
	var updateArgs []interface{}
	reflection := message.ProtoReflect()

	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		if !reflection.Has(fieldDesc) {
			continue
		}
		updateClauses = append(updateClauses, fmt.Sprintf("%s = ?", string(fieldDesc.Name())))
		updateArgs = append(updateArgs, SerializeFieldAsString(message, fieldDesc))
	}

	updateClauseStr := strings.Join(updateClauses, ", ")
	fullSQL := fmt.Sprintf("%s ON DUPLICATE KEY UPDATE %s", insertSQL.Sql, updateClauseStr)
	fullArgs := append(insertSQL.Args, updateArgs...)

	return &SqlWithArgs{Sql: fullSQL, Args: fullArgs}
}

// GetInsertOnDupKeyForPrimaryKeyWithArgs 生成参数化的INSERT...更新主键语句
func (m *MessageTable) GetInsertOnDupKeyForPrimaryKeyWithArgs(message proto.Message) *SqlWithArgs {
	if m.primaryKeyField == nil {
		return nil
	}

	insertSQL := m.GetInsertSQLWithArgs(message)
	if insertSQL == nil {
		return nil
	}

	primaryKeyName := string(m.primaryKeyField.Name())
	primaryKeyValue := SerializeFieldAsString(message, m.primaryKeyField)
	updateClause := fmt.Sprintf("%s = ?", primaryKeyName)
	fullSQL := fmt.Sprintf("%s ON DUPLICATE KEY UPDATE %s", insertSQL.Sql, updateClause)
	fullArgs := append(insertSQL.Args, primaryKeyValue)

	return &SqlWithArgs{Sql: fullSQL, Args: fullArgs}
}

// Insert 执行参数化的INSERT操作
func (p *PbMysqlDB) Insert(message proto.Message) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs := table.GetInsertSQLWithArgs(message)
	if sqlWithArgs == nil {
		return fmt.Errorf("failed to generate insert SQL")
	}

	_, err := p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec insert SQL failed: sql=%s, args=%v, err=%v",
			sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}

	return nil
}

// InsertOnDupUpdate 执行参数化的INSERT...ON DUPLICATE KEY UPDATE操作
func (p *PbMysqlDB) InsertOnDupUpdate(message proto.Message) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs := table.GetInsertOnDupUpdateSQLWithArgs(message)
	if sqlWithArgs == nil {
		return fmt.Errorf("failed to generate insert on dup update SQL")
	}

	_, err := p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec insert on dup update SQL failed: sql=%s, args=%v, err=%v",
			sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}

	return nil
}

// GetSelectSQLByKVWithArgs 生成参数化的KV查询语句
func (m *MessageTable) GetSelectSQLByKVWithArgs(whereKey, whereVal string) *SqlWithArgs {
	sql := fmt.Sprintf("%s WHERE %s = ?;", m.selectFieldsSQL, whereKey)
	return &SqlWithArgs{Sql: sql, Args: []interface{}{whereVal}}
}

// GetSelectSQLByWhereWithArgs 生成参数化的自定义WHERE查询语句
func (m *MessageTable) GetSelectSQLByWhereWithArgs(whereClause string, whereArgs []interface{}) *SqlWithArgs {
	sql := fmt.Sprintf("%s WHERE %s;", m.selectFieldsSQL, whereClause)
	return &SqlWithArgs{Sql: sql, Args: whereArgs}
}

// GetSelectSQL 合并版：生成查询语句（控制是否带分号）
func (m *MessageTable) GetSelectSQL(includeSemicolon bool) string {
	if includeSemicolon {
		return m.selectAllSQLWithSemicolon
	}
	return m.selectAllSQLWithoutSemicolon
}

// GetDeleteSQLWithArgs 生成参数化的按主键删除语句
func (m *MessageTable) GetDeleteSQLWithArgs(message proto.Message) *SqlWithArgs {
	if m.primaryKeyField == nil {
		return nil
	}
	return &SqlWithArgs{
		Sql:  m.deleteByPKTemplate,
		Args: []interface{}{SerializeFieldAsString(message, m.primaryKeyField)},
	}
}

// GetDeleteSQLByWhereWithArgs 生成参数化的自定义WHERE删除语句
func (m *MessageTable) GetDeleteSQLByWhereWithArgs(whereClause string, whereArgs []interface{}) *SqlWithArgs {
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s", m.tableName, whereClause)
	return &SqlWithArgs{Sql: sql, Args: whereArgs}
}

// Delete 执行参数化的按主键删除操作
func (p *PbMysqlDB) Delete(message proto.Message) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs := table.GetDeleteSQLWithArgs(message)
	if sqlWithArgs == nil {
		return ErrPrimaryKeyNotFound
	}

	_, err := p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec delete SQL failed: sql=%s, args=%v, err=%v",
			sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}

	return nil
}

// GetReplaceSQLWithArgs 生成参数化的REPLACE语句
func (m *MessageTable) GetReplaceSQLWithArgs(message proto.Message) *SqlWithArgs {
	var args []interface{}
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		args = append(args, SerializeFieldAsString(message, fieldDesc))
	}

	placeholders := strings.Repeat("?, ", len(args))
	placeholders = strings.TrimSuffix(placeholders, ", ")
	sqlStmt := fmt.Sprintf("%s%s)", m.replaceSQL, placeholders)

	return &SqlWithArgs{Sql: sqlStmt, Args: args}
}

// GetUpdateSetWithArgs 生成参数化的SET子句和参数
func (m *MessageTable) GetUpdateSetWithArgs(message proto.Message) (setClause string, args []interface{}) {
	needComma := false
	reflection := message.ProtoReflect()

	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		if !reflection.Has(field) {
			continue
		}

		if needComma {
			setClause += ", "
		} else {
			needComma = true
		}

		setClause += fmt.Sprintf(" %s = ?", string(field.Name()))
		args = append(args, SerializeFieldAsString(message, field))
	}

	return setClause, args
}

// GetUpdateSQLWithArgs 生成参数化的按主键更新语句
func (m *MessageTable) GetUpdateSQLWithArgs(message proto.Message) *SqlWithArgs {
	setClause, setArgs := m.GetUpdateSetWithArgs(message)
	if setClause == "" {
		return nil
	}

	var whereClause string
	var whereArgs []interface{}
	needComma := false

	for _, primaryKey := range m.primaryKey {
		field := m.Descriptor.Fields().ByName(protoreflect.Name(primaryKey))
		if field == nil {
			continue
		}

		if needComma {
			whereClause += " AND "
		} else {
			needComma = true
		}

		whereClause += fmt.Sprintf("%s = ?", primaryKey)
		whereArgs = append(whereArgs, SerializeFieldAsString(message, field))
	}

	if whereClause == "" {
		return nil
	}

	fullSQL := fmt.Sprintf("UPDATE %s SET %s WHERE %s", m.tableName, setClause, whereClause)
	fullArgs := append(setArgs, whereArgs...)

	return &SqlWithArgs{Sql: fullSQL, Args: fullArgs}
}

// GetUpdateSQLByWhereWithArgs 生成参数化的自定义WHERE更新语句
func (m *MessageTable) GetUpdateSQLByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) *SqlWithArgs {
	setClause, setArgs := m.GetUpdateSetWithArgs(message)
	if setClause == "" {
		return nil
	}

	fullSQL := fmt.Sprintf("UPDATE %s SET %s WHERE %s", m.tableName, setClause, whereClause)
	fullArgs := append(setArgs, whereArgs...)

	return &SqlWithArgs{Sql: fullSQL, Args: fullArgs}
}

// Update 执行参数化的按主键更新操作
func (p *PbMysqlDB) Update(message proto.Message) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs := table.GetUpdateSQLWithArgs(message)
	if sqlWithArgs == nil {
		return fmt.Errorf("failed to generate update SQL (no fields or missing primary key)")
	}

	_, err := p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec update SQL failed: sql=%s, args=%v, err=%v",
			sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}

	return nil
}

// Init 初始化MessageTable的SQL片段
func (m *MessageTable) Init() {
	// 构建字段列表
	needComma := false
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		if needComma {
			m.fieldsListSQL += ", "
		} else {
			needComma = true
		}
		m.fieldsListSQL += string(m.Descriptor.Fields().Get(i).Name())
	}

	// 初始化查询相关SQL片段
	m.selectFieldsSQL = "SELECT " + m.fieldsListSQL + " FROM " + m.tableName
	m.selectAllSQLWithSemicolon = m.selectFieldsSQL + ";"
	m.selectAllSQLWithoutSemicolon = m.selectFieldsSQL + " "

	// 初始化插入/替换相关SQL片段
	placeholders := strings.Repeat("?, ", strings.Count(m.fieldsListSQL, ",")+1)
	placeholders = strings.TrimSuffix(placeholders, ", ")
	m.insertSQLTemplate = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", m.tableName, m.fieldsListSQL, placeholders)
	m.replaceSQL = "REPLACE INTO " + m.tableName + " (" + m.fieldsListSQL + ") VALUES ("
	m.insertSQL = "INSERT INTO " + m.tableName + " (" + m.fieldsListSQL + ") VALUES "

	// 初始化主键字段描述符
	if len(m.primaryKey) > 0 {
		m.primaryKeyField = m.Descriptor.Fields().ByName(protoreflect.Name(m.primaryKey[PrimaryKeyIndex]))
		// 缓存按主键删除模板
		if m.primaryKeyField != nil {
			m.deleteByPKTemplate = fmt.Sprintf("DELETE FROM %s WHERE %s = ?",
				m.tableName, string(m.primaryKeyField.Name()))
		}
	}
}

func NewPbMysqlDB() *PbMysqlDB {
	return &PbMysqlDB{
		Tables: make(map[string]*MessageTable),
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

// Save 执行参数化的REPLACE操作
func (p *PbMysqlDB) Save(message proto.Message) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs := table.GetReplaceSQLWithArgs(message)
	if sqlWithArgs == nil {
		return fmt.Errorf("failed to generate replace SQL")
	}

	_, err := p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec replace SQL failed: sql=%s, args=%v, err=%v",
			sqlWithArgs.Sql, sqlWithArgs.Args, err)
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
		return fmt.Errorf("exec select SQL failed: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	columnValues := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k := range columnValues {
		scans[k] = &columnValues[k]
	}

	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}

		result := make([]string, len(columns))
		for i, v := range columnValues {
			result[i] = string(v)
		}

		if err := ParseFromString(message, result); err != nil {
			return err
		}
	}

	return nil
}

// FindAll 执行全表查询（批量数据）
func (p *PbMysqlDB) FindAll(message proto.Message) error {
	reflectionParent := message.ProtoReflect()
	md := reflectionParent.Descriptor()
	fieldDescriptors := md.Fields()

	// 校验列表消息结构
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

	rows, err := p.DB.Query(table.GetSelectSQL(true))
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	columnValues := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k := range columnValues {
		scans[k] = &columnValues[k]
	}

	listValue := reflectionParent.Mutable(listField).List()
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}

		result := make([]string, len(columns))
		for i, v := range columnValues {
			result[i] = string(v)
		}

		listElement := listValue.NewElement()
		if err := ParseFromString(listElement.Message().Interface(), result); err != nil {
			return err
		}
		listValue.Append(listElement)
	}

	return nil
}

// FindAllByWhereWithArgs 执行参数化的自定义WHERE查询（批量数据）
func (p *PbMysqlDB) FindAllByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error {
	reflectionParent := message.ProtoReflect()
	md := reflectionParent.Descriptor()
	fieldDescriptors := md.Fields()

	// 校验列表消息结构
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
		return fmt.Errorf("exec select all SQL failed: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	columnValues := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k := range columnValues {
		scans[k] = &columnValues[k]
	}

	listValue := reflectionParent.Mutable(listField).List()
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}

		result := make([]string, len(columns))
		for i, v := range columnValues {
			result[i] = string(v)
		}

		listElement := listValue.NewElement()
		if err := ParseFromString(listElement.Message().Interface(), result); err != nil {
			return err
		}
		listValue.Append(listElement)
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
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	columnValues := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k := range columnValues {
		scans[k] = &columnValues[k]
	}

	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}

		result := make([]string, len(columns))
		for i, v := range columnValues {
			result[i] = string(v)
		}

		if err := ParseFromString(message, result); err != nil {
			return err
		}
	}

	return nil
}

// FindAllByWhereClause 兼容原有非参数化批量查询
func (p *PbMysqlDB) FindAllByWhereClause(message proto.Message, whereClause string) error {
	reflectionParent := message.ProtoReflect()
	md := reflectionParent.Descriptor()
	fieldDescriptors := md.Fields()

	// 校验列表消息结构
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
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	columnValues := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k := range columnValues {
		scans[k] = &columnValues[k]
	}

	listValue := reflectionParent.Mutable(listField).List()
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}

		result := make([]string, len(columns))
		for i, v := range columnValues {
			result[i] = string(v)
		}

		listElement := listValue.NewElement()
		if err := ParseFromString(listElement.Message().Interface(), result); err != nil {
			return err
		}
		listValue.Append(listElement)
	}

	return nil
}

// RegisterTable 注册Protobuf与表的映射关系
func (p *PbMysqlDB) RegisterTable(m proto.Message) {
	tableName := GetTableName(m)
	p.Tables[tableName] = &MessageTable{
		tableName:       tableName,
		defaultInstance: m,
		Descriptor:      GetDescriptor(m),
		options:         GetDescriptor(m).Options().ProtoReflect(),
		fields:          make(map[int]string),
	}

	table, ok := p.Tables[tableName]
	if ok {
		table.Init()
	}
}

// Close 关闭数据库连接
func (p *PbMysqlDB) Close() {
	if err := p.DB.Close(); err != nil {
		log.Fatalf("close db failed: %v", err)
	}
}
