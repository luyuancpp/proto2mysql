package protobuf_to_mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/luyuancpp/dbprotooption"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"log"
	"strconv"
	"strings"
)

// 常量：修正命名规范，移除k前缀
const PrimaryKeyIndex = 0

// SqlWithArgs 存储带?占位符的SQL和对应的参数列表
type SqlWithArgs struct {
	Sql  string        // 带占位符的SQL（如 "REPLACE INTO t (a,b) VALUES (?,?)"）
	Args []interface{} // 与占位符一一对应的参数值
}

type MessageTable struct {
	tableName                    string
	defaultInstance              proto.Message
	options                      protoreflect.Message
	primaryKeyField              protoreflect.FieldDescriptor
	autoIncrement                uint64
	fields                       map[int]string
	primaryKey                   []string
	indexes                      []string
	uniqueKeys                   string
	autoIncreaseKey              string
	Descriptor                   protoreflect.MessageDescriptor
	DB                           *sql.DB
	selectAllSQLWithSemicolon    string // 修正命名：明确带分号
	selectAllSQLWithoutSemicolon string // 修正命名：明确不带分号

	selectFieldsSQL string // 合并精简：原getSelectFieldsFromTableSqlStmt
	fieldsListSQL   string // 修正命名：原getFieldsSqlStmt
	replaceSQL      string
	insertSQL       string
}

func (m *MessageTable) SetAutoIncrement(autoIncrement uint64) {
	m.autoIncrement = autoIncrement
}

func (m *MessageTable) DefaultInstance() proto.Message {
	return m.defaultInstance
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

// 序列化字段（map和list使用Protobuf二进制）
func SerializeFieldAsString(message proto.Message, fieldDesc protoreflect.FieldDescriptor) string {
	reflection := message.ProtoReflect()

	// 处理map类型
	if fieldDesc.IsMap() {
		if !reflection.Has(fieldDesc) {
			return "" // 空map存储为空字节
		}
		mapWrapper := &MapWrapper{MapData: reflection.Get(fieldDesc).Map()}
		data, err := proto.Marshal(mapWrapper)
		if err != nil {
			return ""
		}
		return string(data)
	}

	// 处理list类型
	if fieldDesc.IsList() {
		if !reflection.Has(fieldDesc) {
			return "" // 空列表存储为空字节
		}
		listWrapper := &ListWrapper{ListData: reflection.Get(fieldDesc).List()}
		data, err := proto.Marshal(listWrapper)
		if err != nil {
			return ""
		}
		return string(data)
	}

	// 非集合类型处理
	switch fieldDesc.Kind() {
	case protoreflect.Int32Kind:
		return fmt.Sprintf("%d", reflection.Get(fieldDesc).Int())
	case protoreflect.Uint32Kind:
		return fmt.Sprintf("%d", reflection.Get(fieldDesc).Uint())
	case protoreflect.FloatKind:
		return fmt.Sprintf("%f", reflection.Get(fieldDesc).Float())
	case protoreflect.StringKind:
		return reflection.Get(fieldDesc).String()
	case protoreflect.Int64Kind:
		return fmt.Sprintf("%d", reflection.Get(fieldDesc).Int())
	case protoreflect.Uint64Kind:
		return fmt.Sprintf("%d", reflection.Get(fieldDesc).Uint())
	case protoreflect.DoubleKind:
		return fmt.Sprintf("%f", reflection.Get(fieldDesc).Float())
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

// 反序列化字段（map和list使用Protobuf二进制）
func ParseFromString(message proto.Message, row []string) error {
	reflection := message.ProtoReflect()
	desc := reflection.Descriptor()

	for i := 0; i < desc.Fields().Len(); i++ {
		if i >= len(row) {
			continue
		}

		fieldDesc := desc.Fields().Get(i)
		fieldValue := row[i]
		if fieldValue == "" {
			continue
		}

		// 处理map类型
		if fieldDesc.IsMap() {
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
			val := false
			if row[i] != "" {
				var err error
				val, err = strconv.ParseBool(row[i])
				if err != nil {
					return fmt.Errorf("parse bool failed: %w", err)
				}
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
		}
	}

	return nil
}

// MapWrapper 临时包装器：用于序列化map（TODO：完善ProtoReflect实现）
type MapWrapper struct {
	MapData protoreflect.Map
}

func (m *MapWrapper) ProtoReflect() protoreflect.Message {
	// 注意：当前为占位实现，实际需根据map类型生成反射逻辑
	return protoreflect.Message(nil)
}

// ListWrapper 临时包装器：用于序列化list（TODO：完善ProtoReflect实现）
type ListWrapper struct {
	ListData protoreflect.List
}

func (l *ListWrapper) ProtoReflect() protoreflect.Message {
	// 注意：当前为占位实现，实际需根据list类型生成反射逻辑
	return protoreflect.Message(nil)
}

// MySQLFieldTypes MySQL字段类型映射表（修正命名：精简且明确）
var MySQLFieldTypes = []string{
	"",
	"double NOT NULL DEFAULT '0'",
	"float NOT NULL DEFAULT '0'",
	"bigint NOT NULL",
	"bigint unsigned NOT NULL",
	"int NOT NULL",
	"bigint NOT NULL",
	"int NOT NULL",
	"bool",
	"varchar(256)",
	"Blob",
	"Blob",
	"Blob",
	"int unsigned NOT NULL",
	"int NOT NULL",
	"bigint NOT NULL",
	"int NOT NULL",
	"bigint NOT NULL",
}

// GetCreateTableSQL 生成创建表的SQL语句（修正命名：统一SQL后缀）
func (m *MessageTable) GetCreateTableSQL() string {
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", m.tableName)

	// 解析表选项
	if m.options.Has(dbprotooption.E_OptionPrimaryKey.TypeDescriptor()) {
		v := m.options.Get(dbprotooption.E_OptionPrimaryKey.TypeDescriptor())
		m.primaryKey = strings.Split(v.String(), ",")
	}
	if m.options.Has(dbprotooption.E_OptionIndex.TypeDescriptor()) {
		v := m.options.Get(dbprotooption.E_OptionIndex.TypeDescriptor())
		m.indexes = strings.Split(v.String(), ",")
	}
	if m.options.Has(dbprotooption.E_OptionUniqueKey.TypeDescriptor()) {
		m.uniqueKeys = m.options.Get(dbprotooption.E_OptionUniqueKey.TypeDescriptor()).String()
	}
	m.autoIncreaseKey = m.options.Get(dbprotooption.E_OptionAutoIncrementKey.TypeDescriptor()).String()

	// 生成字段定义
	needComma := false
	fieldIndent := "\t"
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		fieldName := string(field.Name())
		fieldType := MySQLFieldTypes[field.Kind()]

		if needComma {
			stmt += ",\n"
		} else {
			needComma = true
		}

		stmt += fieldIndent + fieldName + " " + fieldType
		if fieldName == m.autoIncreaseKey {
			stmt += " AUTO_INCREMENT"
		}
	}

	// 生成主键约束
	if len(m.primaryKey) > 0 {
		stmt += ",\n" + fieldIndent + fmt.Sprintf("PRIMARY KEY (%s)", m.primaryKey[PrimaryKeyIndex])
	}

	// 生成唯一键约束
	if len(m.uniqueKeys) > 0 {
		stmt += ",\n" + fieldIndent + fmt.Sprintf("UNIQUE KEY (%s)", m.uniqueKeys)
	}

	// 生成普通索引
	for _, index := range m.indexes {
		if index == "" {
			continue
		}
		stmt += ",\n" + fieldIndent + fmt.Sprintf("INDEX (%s)", index)
	}

	// 表选项
	stmt += "\n) ENGINE = INNODB"
	if m.autoIncreaseKey != "" {
		stmt += " AUTO_INCREMENT=1"
	}
	stmt += " DEFAULT CHARSET = utf8mb4;"

	return stmt
}

// GetAlterTableAddFieldSQL 生成添加字段的ALTER语句（删除冗余的1版本）
func (m *MessageTable) GetAlterTableAddFieldSQL() string {
	stmt := "ALTER TABLE " + m.tableName
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		sqlFieldName, ok := m.fields[i]
		fieldName := string(field.Name())
		if ok && sqlFieldName == fieldName {
			continue
		}
		stmt += " ADD COLUMN " + fieldName + " " + MySQLFieldTypes[field.Kind()]
		if i+1 < m.Descriptor.Fields().Len() {
			stmt += ","
		}
	}
	stmt += ";"
	return stmt
}

// GetInsertSQLWithArgs 生成参数化的INSERT语句
func (m *MessageTable) GetInsertSQLWithArgs(message proto.Message) *SqlWithArgs {
	var args []interface{}
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		args = append(args, SerializeFieldAsString(message, fieldDesc))
	}

	placeholders := strings.Repeat("?, ", len(args))
	placeholders = strings.TrimSuffix(placeholders, ", ")
	sqlStmt := fmt.Sprintf("%s(%s)", m.insertSQL, placeholders)

	return &SqlWithArgs{Sql: sqlStmt, Args: args}
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
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found: %s", GetTableName(message))
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
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found: %s", GetTableName(message))
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
	primaryKeyName := string(m.Descriptor.Fields().Get(PrimaryKeyIndex).Name())
	primaryKeyValue := SerializeFieldAsString(message, m.primaryKeyField)

	sql := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", m.tableName, primaryKeyName)
	return &SqlWithArgs{Sql: sql, Args: []interface{}{primaryKeyValue}}
}

// GetDeleteSQLByWhereWithArgs 生成参数化的自定义WHERE删除语句
func (m *MessageTable) GetDeleteSQLByWhereWithArgs(whereClause string, whereArgs []interface{}) *SqlWithArgs {
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s", m.tableName, whereClause)
	return &SqlWithArgs{Sql: sql, Args: whereArgs}
}

// Delete 执行参数化的按主键删除操作
func (p *PbMysqlDB) Delete(message proto.Message) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found: %s", GetTableName(message))
	}

	sqlWithArgs := table.GetDeleteSQLWithArgs(message)
	if sqlWithArgs == nil {
		return fmt.Errorf("failed to generate delete SQL")
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
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found: %s", GetTableName(message))
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
	m.replaceSQL = "REPLACE INTO " + m.tableName + " (" + m.fieldsListSQL + ") VALUES ("
	m.insertSQL = "INSERT INTO " + m.tableName + " (" + m.fieldsListSQL + ") VALUES "
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
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return ""
	}
	return table.GetCreateTableSQL()
}

// GetAlterTableModifyFieldSQL 生成修改字段的ALTER语句
func (m *MessageTable) GetAlterTableModifyFieldSQL() string {
	stmt := "ALTER TABLE " + m.tableName
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		sqlFieldName, ok := m.fields[i]
		fieldName := string(field.Name())
		if ok && sqlFieldName == fieldName {
			continue
		}
		stmt += " CHANGE COLUMN " + sqlFieldName + " " + fieldName + " " + MySQLFieldTypes[field.Kind()] + ","
	}
	stmt = strings.TrimSuffix(stmt, ",")
	stmt += ";"
	return stmt
}

// UpdateTableField 批量更新表字段（先修改再添加）
func (p *PbMysqlDB) UpdateTableField(message proto.Message) error {
	if err := p.AlterModifyTableField(message); err != nil {
		return err
	}
	return p.AlterAddTableField(message)
}

// AlterAddTableField 执行添加字段的ALTER操作
func (p *PbMysqlDB) AlterAddTableField(message proto.Message) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found")
	}

	query := fmt.Sprintf("SELECT COLUMN_NAME,ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s';",
		p.DBName, table.tableName)
	rows, err := p.DB.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	fieldIndex := 0
	var fieldName string
	for rows.Next() {
		if err := rows.Scan(&fieldName, &fieldIndex); err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}
		table.fields[fieldIndex-1] = fieldName
	}

	_, err = p.DB.Exec(table.GetAlterTableAddFieldSQL())
	return err
}

// AlterModifyTableField 执行修改字段的ALTER操作
func (p *PbMysqlDB) AlterModifyTableField(message proto.Message) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found")
	}

	query := fmt.Sprintf("SELECT COLUMN_NAME,ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s';",
		p.DBName, table.tableName)
	rows, err := p.DB.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	fieldIndex := 0
	var fieldName string
	for rows.Next() {
		if err := rows.Scan(&fieldName, &fieldIndex); err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}
		table.fields[fieldIndex-1] = fieldName
	}

	_, err = p.DB.Exec(table.GetAlterTableModifyFieldSQL())
	return err
}

// Save 执行参数化的REPLACE操作
func (p *PbMysqlDB) Save(message proto.Message) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found: %s", GetTableName(message))
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

// FindOneByKV 执行参数化的KV查询（内部复用FindOneByWhereWithArgs）
func (p *PbMysqlDB) FindOneByKV(message proto.Message, whereKey string, whereVal string) error {
	return p.FindOneByWhereWithArgs(message, whereKey+" = ?", []interface{}{whereVal})
}

// FindOneByWhereWithArgs 执行参数化的自定义WHERE查询（单条数据）
func (p *PbMysqlDB) FindOneByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found")
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
	listField := fieldDescriptors.Get(0)
	tableName := string(listField.Message().Name())
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("table not found")
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
	listField := fieldDescriptors.Get(0)
	tableName := string(listField.Message().Name())
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("table not found")
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

// FindOneByWhereClause 兼容原有非参数化查询（不推荐新用，修正命名）
func (p *PbMysqlDB) FindOneByWhereClause(message proto.Message, whereClause string) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found")
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

// FindAllByWhereClause 兼容原有非参数化批量查询（不推荐新用，修正命名）
func (p *PbMysqlDB) FindAllByWhereClause(message proto.Message, whereClause string) error {
	reflectionParent := message.ProtoReflect()
	md := reflectionParent.Descriptor()
	fieldDescriptors := md.Fields()
	listField := fieldDescriptors.Get(0)
	tableName := string(listField.Message().Name())
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("table not found")
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
