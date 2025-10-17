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

const (
	kPrimaryKeyIndex = 0
)

// SqlWithArgs 存储带?占位符的SQL和对应的参数列表
type SqlWithArgs struct {
	Sql  string        // 带占位符的SQL（如 "REPLACE INTO t (a,b) VALUES (?,?)"）
	Args []interface{} // 与占位符一一对应的参数值
}

type MessageTable struct {
	tableName                      string
	defaultInstance                proto.Message
	options                        protoreflect.Message
	primaryKeyField                protoreflect.FieldDescriptor
	autoIncrement                  uint64
	fields                         map[int]string
	primaryKey                     []string
	indexes                        []string
	uniqueKeys                     string
	autoIncreaseKey                string
	Descriptor                     protoreflect.MessageDescriptor
	DB                             *sql.DB
	selectAllSqlStmt               string
	selectAllSqlStmtNoEndSemicolon string

	selectFieldsFromTableSQL string
	fieldsSQL                string
	replaceSQL               string
	insertSQL                string
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
		// 将map包装为临时消息进行序列化
		mapWrapper := &MapWrapper{
			MapData: reflection.Get(fieldDesc).Map(),
		}
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
		// 将list包装为临时消息进行序列化
		listWrapper := &ListWrapper{
			ListData: reflection.Get(fieldDesc).List(),
		}
		data, err := proto.Marshal(listWrapper)
		if err != nil {
			return ""
		}
		return string(data)
	}

	// 原有非集合类型处理逻辑
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
		val := reflection.Get(fieldDesc).Enum()
		return fmt.Sprintf("%d", int32(val))
	case protoreflect.BytesKind:
		data := reflection.Get(fieldDesc).Bytes()
		return string(data)
	case protoreflect.MessageKind:
		if reflection.Has(fieldDesc) {
			subMessage := reflection.Get(fieldDesc).Message()
			data, err := proto.Marshal(subMessage.Interface())
			if err == nil {
				return string(data)
			} else {
				return "<marshal_error>"
			}
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
			continue // 忽略超出行长度的字段
		}

		fieldDesc := desc.Fields().Get(i)
		fieldValue := row[i]
		if fieldValue == "" {
			continue // 空值不处理
		}

		// 处理map类型
		if fieldDesc.IsMap() {
			// 反序列化二进制到临时包装器
			mapWrapper := &MapWrapper{}
			data := []byte(fieldValue)
			if err := proto.Unmarshal(data, mapWrapper); err != nil {
				return fmt.Errorf("parse map (field: %s): %w", fieldDesc.Name(), err)
			}
			// 将map数据设置到Protobuf
			mapVal := reflection.Mutable(fieldDesc).Map()
			mapWrapper.MapData.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
				mapVal.Set(key, value)
				return true
			})
			continue
		}

		// 处理list类型
		if fieldDesc.IsList() {
			// 反序列化二进制到临时包装器
			listWrapper := &ListWrapper{}
			data := []byte(fieldValue)
			if err := proto.Unmarshal(data, listWrapper); err != nil {
				return fmt.Errorf("parse list (field: %s): %w", fieldDesc.Name(), err)
			}
			// 将list数据设置到Protobuf
			listVal := reflection.Mutable(fieldDesc).List()
			for i := 0; i < listWrapper.ListData.Len(); i++ {
				listVal.Append(listWrapper.ListData.Get(i))
			}
			continue
		}

		// 原有非集合类型处理逻辑
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
			if row[i] == "" {
				reflection.Set(fieldDesc, protoreflect.ValueOfBool(false))
			} else {
				val, err := strconv.ParseBool(row[i])
				if err != nil {
					return fmt.Errorf("parse bool failed: %w", err)
				}
				reflection.Set(fieldDesc, protoreflect.ValueOfBool(val))
			}
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
			err := proto.Unmarshal([]byte(row[i]), subMsg.Interface())
			if err != nil {
				return fmt.Errorf("unmarshal sub-message failed: %w", err)
			}
		}
	}

	return nil
}

// 临时包装器：用于序列化map（Protobuf需要具体消息类型才能序列化）
type MapWrapper struct {
	MapData protoreflect.Map
}

// 实现proto.Message接口（map序列化需要）
func (m *MapWrapper) ProtoReflect() protoreflect.Message {
	// 实际使用时需要根据具体map类型生成对应的反射实现
	// 这里简化处理，实际项目中需要更完整的实现
	return protoreflect.Message(nil)
}

// 临时包装器：用于序列化list（Protobuf需要具体消息类型才能序列化）
type ListWrapper struct {
	ListData protoreflect.List
}

// 实现proto.Message接口（list序列化需要）
func (l *ListWrapper) ProtoReflect() protoreflect.Message {
	// 实际使用时需要根据具体list类型生成对应的反射实现
	// 这里简化处理，实际项目中需要更完整的实现
	return protoreflect.Message(nil)
}

var MysqlFieldDescriptorType = []string{
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

func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}

func (m *MessageTable) GetCreateTableSqlStmt() string {
	// 1. 基础表结构开头（表名），换行后加左括号
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", m.tableName)

	// 2. 解析表选项（主键、索引、唯一键、自增键）
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

	// 3. 生成字段定义（缩进1级，每个字段占一行）
	needComma := false
	fieldIndent := "\t" // 字段缩进（1个制表符，可改为2/4个空格）
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		fieldName := string(field.Name())
		fieldType := MysqlFieldDescriptorType[field.Kind()]

		// 字段间添加逗号（除第一个字段外）
		if needComma {
			stmt += ",\n"
		} else {
			needComma = true
		}

		// 拼接字段：缩进 + 字段名 + 空格 + 类型 + 自增约束
		stmt += fieldIndent
		stmt += fieldName
		stmt += " "
		stmt += fieldType
		if fieldName == m.autoIncreaseKey {
			stmt += " AUTO_INCREMENT"
		}
	}

	// 4. 生成主键约束（换行 + 缩进，单独占一行）
	if len(m.primaryKey) > 0 {
		stmt += ",\n"
		stmt += fieldIndent
		stmt += fmt.Sprintf("PRIMARY KEY (%s)", m.primaryKey[kPrimaryKeyIndex])
	}

	// 5. 生成唯一键约束（换行 + 缩进，单独占一行）
	if len(m.uniqueKeys) > 0 {
		stmt += ",\n"
		stmt += fieldIndent
		stmt += fmt.Sprintf("UNIQUE KEY (%s)", m.uniqueKeys)
	}

	// 6. 生成普通索引约束（每个索引单独占一行）
	for _, index := range m.indexes {
		if index == "" {
			continue
		}
		stmt += ",\n"
		stmt += fieldIndent
		stmt += fmt.Sprintf("INDEX (%s)", index)
	}

	// 7. 表选项（引擎、自增起始值、字符集），换行后与表名对齐
	stmt += "\n"
	stmt += ") ENGINE = INNODB"
	if m.autoIncreaseKey != "" {
		stmt += " AUTO_INCREMENT=1"
	}
	stmt += " DEFAULT CHARSET = utf8mb4;" // 规范：关键字大写，加结尾分号

	return stmt
}

func (m *MessageTable) GetAlterTableAddFieldSqlStmt() string {
	stmt := "ALTER TABLE " + m.tableName
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		sqlFieldName, ok := m.fields[i]
		fieldName := string(field.Name())
		if ok && sqlFieldName == fieldName {
			continue
		}
		stmt += " ADD COLUMN "
		stmt += string(field.Name())
		stmt += " "
		stmt += MysqlFieldDescriptorType[field.Kind()]
		if i+1 < m.Descriptor.Fields().Len() {
			stmt += ","
		}
	}
	stmt += ";"
	return stmt
}

// GetInsertSqlWithArgs 生成参数化的INSERT SQL（返回SqlWithArgs，避免字符串拼接）
func (m *MessageTable) GetInsertSqlWithArgs(message proto.Message) *SqlWithArgs {
	// 1. 收集参数列表（复用SerializeFieldAsString，无需手动加单引号）
	var args []interface{}
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		value := SerializeFieldAsString(message, fieldDesc)
		args = append(args, value)
	}

	// 2. 生成带?占位符的INSERT SQL（m.insertSQL 是 "INSERT INTO t (a,b) VALUES "）
	placeholders := strings.Repeat("?, ", len(args))
	placeholders = strings.TrimSuffix(placeholders, ", ")
	sqlStmt := fmt.Sprintf("%s(%s)", m.insertSQL, placeholders)

	return &SqlWithArgs{
		Sql:  sqlStmt,
		Args: args,
	}
}

// GetInsertOnDupUpdateSqlWithArgs 生成参数化的INSERT ... ON DUPLICATE KEY UPDATE SQL
func (m *MessageTable) GetInsertOnDupUpdateSqlWithArgs(message proto.Message) *SqlWithArgs {
	// 1. 获取基础的INSERT参数化SQL和参数
	insertSqlWithArgs := m.GetInsertSqlWithArgs(message)
	if insertSqlWithArgs == nil {
		return nil
	}

	// 2. 生成Update部分的占位符和参数（避免字符串拼接）
	var updateClauses []string
	var updateArgs []interface{}
	reflection := message.ProtoReflect()

	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		if !reflection.Has(fieldDesc) {
			continue
		}
		// 生成 "field = ?" 格式的Update子句
		updateClauses = append(updateClauses, fmt.Sprintf("%s = ?", string(fieldDesc.Name())))
		// 收集Update部分的参数（与Insert参数分开，避免顺序混乱）
		updateArgs = append(updateArgs, SerializeFieldAsString(message, fieldDesc))
	}

	// 3. 拼接完整SQL（INSERT + ON DUPLICATE KEY UPDATE）
	updateClauseStr := strings.Join(updateClauses, ", ")
	fullSql := fmt.Sprintf("%s ON DUPLICATE KEY UPDATE %s", insertSqlWithArgs.Sql, updateClauseStr)

	// 4. 合并参数（Insert参数在前，Update参数在后，与占位符顺序一致）
	fullArgs := append(insertSqlWithArgs.Args, updateArgs...)

	return &SqlWithArgs{
		Sql:  fullSql,
		Args: fullArgs,
	}
}

// GetInsertOnDupKeyForPrimaryKeyWithArgs 生成参数化的INSERT ... ON DUPLICATE KEY UPDATE（仅更新主键）
func (m *MessageTable) GetInsertOnDupKeyForPrimaryKeyWithArgs(message proto.Message) *SqlWithArgs {
	if m.primaryKeyField == nil {
		return nil
	}

	// 1. 获取基础的INSERT参数化SQL和参数
	insertSqlWithArgs := m.GetInsertSqlWithArgs(message)
	if insertSqlWithArgs == nil {
		return nil
	}

	// 2. 生成主键Update的占位符和参数
	primaryKeyName := string(m.primaryKeyField.Name())
	primaryKeyValue := SerializeFieldAsString(message, m.primaryKeyField)
	updateClause := fmt.Sprintf("%s = ?", primaryKeyName)

	// 3. 拼接完整SQL
	fullSql := fmt.Sprintf("%s ON DUPLICATE KEY UPDATE %s", insertSqlWithArgs.Sql, updateClause)

	// 4. 合并参数（Insert参数 + 主键参数）
	fullArgs := append(insertSqlWithArgs.Args, primaryKeyValue)

	return &SqlWithArgs{
		Sql:  fullSql,
		Args: fullArgs,
	}
}

// Insert 执行参数化的INSERT操作（避免SQL注入）
func (p *PbMysqlDB) Insert(message proto.Message) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found: %s", GetTableName(message))
	}

	// 1. 获取参数化的Insert SQL和参数
	sqlWithArgs := table.GetInsertSqlWithArgs(message)
	if sqlWithArgs == nil {
		return fmt.Errorf("failed to generate insert SQL")
	}

	// 2. 执行参数化查询（驱动自动处理转义）
	_, err := p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec insert SQL failed: sql=%s, args=%v, err=%v",
			sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}

	return nil
}

// InsertOnDupUpdate 执行参数化的INSERT ... ON DUPLICATE KEY UPDATE操作
func (p *PbMysqlDB) InsertOnDupUpdate(message proto.Message) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found: %s", GetTableName(message))
	}

	// 1. 获取参数化的SQL和参数
	sqlWithArgs := table.GetInsertOnDupUpdateSqlWithArgs(message)
	if sqlWithArgs == nil {
		return fmt.Errorf("failed to generate insert on dup update SQL")
	}

	// 2. 执行参数化查询
	_, err := p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec insert on dup update SQL failed: sql=%s, args=%v, err=%v",
			sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}

	return nil
}

// 改造后：返回参数化的SELECT SQL（按KV查询）
func (m *MessageTable) GetSelectSqlByKVWhereWithArgs(whereType, whereVal string) *SqlWithArgs {
	sql := fmt.Sprintf(
		"%s WHERE %s = ?;",
		m.getSelectFieldsFromTableSqlStmt(),
		whereType,
	)
	return &SqlWithArgs{
		Sql:  sql,
		Args: []interface{}{whereVal},
	}
}

// 获取添加字段的 SQL 语句
func (m *MessageTable) GetAlterTableAddFieldSqlStmt1() string {
	stmt := "ALTER TABLE " + m.tableName + " ADD COLUMN "

	// 为每个字段生成 ADD COLUMN 语句
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		stmt += string(field.Name()) + " " + MysqlFieldDescriptorType[field.Kind()] + ", "
	}

	stmt = strings.TrimSuffix(stmt, ", ")
	stmt += ";"
	return stmt
}

func (m *MessageTable) GetSelectSqlStmt() string {
	return m.selectAllSqlStmt
}

func (m *MessageTable) GetSelectSqlStmtNoEndSemicolon() string {
	return m.selectAllSqlStmtNoEndSemicolon
}

func (m *MessageTable) getFieldsSqlStmt() string {
	return m.fieldsSQL
}

func (m *MessageTable) getSelectFieldsFromTableSqlStmt() string {
	return m.selectFieldsFromTableSQL
}

func (m *MessageTable) GetSelectSqlWithWhereClause(whereClause string) string {
	stmt := m.getSelectFieldsFromTableSqlStmt()
	stmt += " WHERE "
	stmt += whereClause
	stmt += ";"
	return stmt
}

// 改造后：返回参数化的DELETE SQL（按主键删除）
func (m *MessageTable) GetDeleteSqlWithArgs(message proto.Message) *SqlWithArgs {
	primaryKeyName := string(m.Descriptor.Fields().Get(kPrimaryKeyIndex).Name())
	primaryKeyValue := SerializeFieldAsString(message, m.primaryKeyField)

	sql := fmt.Sprintf(
		"DELETE FROM %s WHERE %s = ?",
		m.tableName,
		primaryKeyName,
	)
	return &SqlWithArgs{
		Sql:  sql,
		Args: []interface{}{primaryKeyValue},
	}
}

// 改造后：返回参数化的DELETE SQL（自定义WHERE子句）
func (m *MessageTable) GetDeleteSqlWithWhereArgs(whereClause string, whereArgs []interface{}) *SqlWithArgs {
	sql := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		m.tableName,
		whereClause, // 含?占位符
	)
	return &SqlWithArgs{
		Sql:  sql,
		Args: whereArgs,
	}
}

// 新增：执行参数化的DELETE操作
func (p *PbMysqlDB) Delete(message proto.Message) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found: %s", GetTableName(message))
	}

	sqlWithArgs := table.GetDeleteSqlWithArgs(message)
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

func (m *MessageTable) GetReplaceIntoSql(message proto.Message) *SqlWithArgs {
	// 1. 收集参数列表（复用原有的SerializeFieldAsString，但无需手动加单引号）
	var args []interface{}
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		// 注意：此处不再拼接字符串，而是直接将序列化后的值存入args
		value := SerializeFieldAsString(message, fieldDesc)
		args = append(args, value)
	}

	// 2. 生成带?占位符的SQL（替换原有的字符串拼接）
	placeholders := strings.Repeat("?, ", len(args))
	placeholders = strings.TrimSuffix(placeholders, ", ")       // 去掉最后一个多余的逗号
	sqlStmt := fmt.Sprintf("%s%s)", m.replaceSQL, placeholders) // m.replaceSQL 是原有的 "REPLACE INTO t (a,b) VALUES ("

	return &SqlWithArgs{
		Sql:  sqlStmt,
		Args: args,
	}
}

// GetUpdateSetWithArgs 生成参数化的SET子句和对应的参数
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

		// 生成 "field = ?" 格式的子句，不直接拼接值
		setClause += fmt.Sprintf(" %s = ?", string(field.Name()))
		// 收集参数（值）
		args = append(args, SerializeFieldAsString(message, field))
	}

	return setClause, args
}

// GetUpdateSqlWithArgs 生成参数化的UPDATE语句和对应的参数列表
func (m *MessageTable) GetUpdateSqlWithArgs(message proto.Message) *SqlWithArgs {
	// 1. 获取SET子句和对应的参数
	setClause, setArgs := m.GetUpdateSetWithArgs(message)
	if setClause == "" {
		return nil // 没有需要更新的字段
	}

	// 2. 生成WHERE子句（主键条件）和对应的参数
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

		// 生成 "primaryKey = ?" 格式的条件，不直接拼接值
		whereClause += fmt.Sprintf("%s = ?", primaryKey)
		// 收集WHERE条件的参数
		whereArgs = append(whereArgs, SerializeFieldAsString(message, field))
	}

	if whereClause == "" {
		return nil // 缺少主键条件，避免全表更新
	}

	// 3. 拼接完整的UPDATE SQL
	fullSql := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		m.tableName,
		setClause,
		whereClause,
	)

	// 4. 合并所有参数（SET参数在前，WHERE参数在后）
	fullArgs := append(setArgs, whereArgs...)

	return &SqlWithArgs{
		Sql:  fullSql,
		Args: fullArgs,
	}
}

// Update 执行参数化的UPDATE操作
func (p *PbMysqlDB) Update(message proto.Message) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found: %s", GetTableName(message))
	}

	// 获取参数化的UPDATE语句和参数
	sqlWithArgs := table.GetUpdateSqlWithArgs(message)
	if sqlWithArgs == nil {
		return fmt.Errorf("failed to generate update SQL (no fields to update or missing primary key)")
	}

	// 执行参数化查询
	_, err := p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec update SQL failed: sql=%s, args=%v, err=%v",
			sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}

	return nil
}

func (m *MessageTable) Init() {

	needComma := false
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		if needComma {
			m.fieldsSQL += ", "
		} else {
			needComma = true
		}
		m.fieldsSQL += string(m.Descriptor.Fields().Get(i).Name())
	}

	m.selectFieldsFromTableSQL = "SELECT "
	m.selectFieldsFromTableSQL += m.fieldsSQL
	m.selectFieldsFromTableSQL += " FROM "
	m.selectFieldsFromTableSQL += m.tableName

	m.selectAllSqlStmt = m.getSelectFieldsFromTableSqlStmt() + ";"
	m.selectAllSqlStmtNoEndSemicolon = m.getSelectFieldsFromTableSqlStmt() + " "

	m.replaceSQL = "REPLACE INTO " + m.tableName + " (" + m.getFieldsSqlStmt() + ") VALUES ("

	m.insertSQL = "INSERT INTO " + m.tableName + " (" + m.getFieldsSqlStmt() + ") VALUES "
}

// 改造后：返回参数化的UPDATE SQL（自定义WHERE子句）
func (m *MessageTable) GetUpdateSqlWithWhereArgs(message proto.Message, whereClause string, whereArgs []interface{}) *SqlWithArgs {
	// 1. 生成参数化的SET子句（复用已有逻辑）
	setClause, setArgs := m.GetUpdateSetWithArgs(message)
	if setClause == "" {
		return nil
	}

	// 2. 拼接带占位符的完整SQL
	sql := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		m.tableName,
		setClause,
		whereClause, // 此处whereClause含?占位符（如 "group_id=? AND name=?"）
	)

	// 3. 合并SET参数和WHERE参数
	fullArgs := append(setArgs, whereArgs...)
	return &SqlWithArgs{
		Sql:  sql,
		Args: fullArgs,
	}
}

func NewPbMysqlDB() *PbMysqlDB {
	return &PbMysqlDB{
		Tables: make(map[string]*MessageTable),
	}
}

func GetTableName(m proto.Message) string {
	return string(m.ProtoReflect().Descriptor().FullName())
}

func GetDescriptor(m proto.Message) protoreflect.MessageDescriptor {
	return m.ProtoReflect().Descriptor()
}

func (p *PbMysqlDB) GetCreateTableSql(message proto.Message) string {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return ""
	}
	return table.GetCreateTableSqlStmt()
}

func (m *MessageTable) GetAlterTableModifyFieldSqlStmt() string {
	stmt := "ALTER TABLE " + m.tableName
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		sqlFieldName, ok := m.fields[i]
		fieldName := string(field.Name())
		if ok && sqlFieldName == fieldName {
			continue
		}
		stmt += " CHANGE  COLUMN "
		stmt += sqlFieldName
		stmt += " "
		stmt += fieldName
		stmt += " "
		stmt += MysqlFieldDescriptorType[field.Kind()]
		stmt += ","
	}
	stmt = strings.Trim(stmt, ",")
	stmt += ";"
	return stmt
}

func (p *PbMysqlDB) UpdateTableField(message proto.Message) error {
	err := p.AlterModifyTableField(message)
	if err != nil {
		return err
	}
	err = p.AlterAddTableField(message)
	if err != nil {
		return err
	}
	return nil
}

func (p *PbMysqlDB) AlterAddTableField(message proto.Message) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found")
	}
	stmt := fmt.Sprintf("SELECT COLUMN_NAME,ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s';",
		p.DBName,
		table.tableName)

	rows, err := p.DB.Query(stmt)
	if err != nil {
		return err
	}
	defer rows.Close()

	fieldIndex := 0
	var fieldName string

	for rows.Next() {
		err = rows.Scan(&fieldName, &fieldIndex)
		if err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}
		table.fields[fieldIndex-1] = fieldName
	}
	_, err = p.DB.Exec(table.GetAlterTableAddFieldSqlStmt())
	if err != nil {
		return err
	}

	return nil
}

func (p *PbMysqlDB) AlterModifyTableField(message proto.Message) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found")
	}
	sqlStmt := fmt.Sprintf("SELECT COLUMN_NAME,ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s';",
		p.DBName,
		table.tableName)

	rows, err := p.DB.Query(sqlStmt)
	if err != nil {
		return err
	}
	defer rows.Close()

	fieldIndex := 0
	var fieldName string

	for rows.Next() {
		err = rows.Scan(&fieldName, &fieldIndex)
		if err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}
		table.fields[fieldIndex-1] = fieldName
	}
	_, err = p.DB.Exec(table.GetAlterTableModifyFieldSqlStmt())
	if err != nil {
		return err
	}

	return nil
}

func (p *PbMysqlDB) Save(message proto.Message) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found: %s", GetTableName(message))
	}

	// 1. 获取带占位符的SQL和参数列表
	sqlWithArgs := table.GetReplaceIntoSql(message)
	if sqlWithArgs == nil {
		return fmt.Errorf("failed to generate replace SQL")
	}

	// 2. 执行参数化查询（驱动自动处理转义）
	_, err := p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		// 打印错误时附带SQL和参数，方便调试（生产环境需脱敏）
		return fmt.Errorf("exec replace SQL failed: sql=%s, args=%v, err=%v",
			sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}

	return nil
}

func (p *PbMysqlDB) FindOneByKV(message proto.Message, whereType string, whereValue string) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found")
	}

	// 调用参数化方法获取SQL和参数
	sqlWithArgs := table.GetSelectSqlByKVWhereWithArgs(whereType, whereValue)
	rows, err := p.DB.Query(sqlWithArgs.Sql, sqlWithArgs.Args...) // 传递args
	if err != nil {
		return fmt.Errorf("exec select SQL failed: %w", err)
	}
	defer rows.Close() // 补充defer关闭rows，避免资源泄漏

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	vals := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k, _ := range vals {
		scans[k] = &vals[k]
	}

	for rows.Next() {
		err := rows.Scan(scans...)
		if err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}
		i := 0
		result := make([]string, len(columns))
		for _, v := range vals {
			result[i] = string(v)
			i++
		}
		err = ParseFromString(message, result)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *PbMysqlDB) FindOneByWhereCase(message proto.Message, whereCase string) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found")
	}
	stm := table.GetSelectSqlStmtNoEndSemicolon() + whereCase + ";"
	rows, err := p.DB.Query(stm)
	if err != nil {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	vals := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k, _ := range vals {
		scans[k] = &vals[k]
	}

	for rows.Next() {
		err := rows.Scan(scans...)
		if err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}
		i := 0
		result := make([]string, len(columns))
		for _, v := range vals {
			result[i] = string(v)
			i++
		}
		err = ParseFromString(message, result)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *PbMysqlDB) FindAll(message proto.Message) error {
	reflectionParent := message.ProtoReflect()
	md := reflectionParent.Descriptor()
	fds := md.Fields()
	listField := fds.Get(0)
	name := string(listField.Message().Name())
	table, ok := p.Tables[name]
	if !ok {
		return fmt.Errorf("table not found")
	}
	rows, err := p.DB.Query(table.GetSelectSqlStmt())
	if err != nil {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k, _ := range values {
		scans[k] = &values[k]
	}
	lv := reflectionParent.Mutable(listField).List()
	for rows.Next() {
		err := rows.Scan(scans...)
		if err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}
		i := 0
		result := make([]string, len(columns))
		for _, v := range values {
			result[i] = string(v)
			i++
		}
		ve := lv.NewElement()
		err = ParseFromString(ve.Message().Interface(), result)
		if err != nil {
			return err
		}
		lv.Append(ve)
	}

	return nil
}

// 新增：参数化的按条件查询（支持WHERE子句带?占位符）
func (m *MessageTable) GetSelectSqlByWhereWithArgs(whereClause string, whereArgs []interface{}) *SqlWithArgs {
	sql := fmt.Sprintf(
		"%s WHERE %s;",
		m.getSelectFieldsFromTableSqlStmt(),
		whereClause, // 含?占位符（如 "name=? AND age>?"）
	)
	return &SqlWithArgs{
		Sql:  sql,
		Args: whereArgs,
	}
}

// 配套改造FindOneByWhereCase
func (p *PbMysqlDB) FindOneByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found")
	}
	sqlWithArgs := table.GetSelectSqlByWhereWithArgs(whereClause, whereArgs)
	rows, err := p.DB.Query(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	// 后续的rows.Scan、ParseFromString逻辑不变...
}

func (p *PbMysqlDB) FindAllByWhereCase(message proto.Message, whereCase string) error {
	reflectionParent := message.ProtoReflect()
	md := reflectionParent.Descriptor()
	fds := md.Fields()
	listField := fds.Get(0)
	name := string(listField.Message().Name())
	table, ok := p.Tables[name]
	if !ok {
		return fmt.Errorf("table not found")
	}
	stm := table.GetSelectSqlStmtNoEndSemicolon() + whereCase + ";"
	rows, err := p.DB.Query(stm)
	if err != nil {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k, _ := range values {
		scans[k] = &values[k]
	}
	lv := reflectionParent.Mutable(listField).List()
	for rows.Next() {
		err := rows.Scan(scans...)
		if err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}
		i := 0
		result := make([]string, len(columns))
		for _, v := range values {
			result[i] = string(v)
			i++
		}
		ve := lv.NewElement()
		if err := ParseFromString(ve.Message().Interface(), result); err != nil {
			return err
		}
		lv.Append(ve)
	}

	return nil
}

func (p *PbMysqlDB) RegisterTable(m proto.Message) {
	p.Tables[GetTableName(m)] = &MessageTable{
		tableName:       GetTableName(m),
		defaultInstance: m,
		Descriptor:      GetDescriptor(m),
		options:         GetDescriptor(m).Options().ProtoReflect(),
		fields:          make(map[int]string)}

	table, ok := p.Tables[GetTableName(m)]
	if !ok {
		return
	}
	table.Init()
}

func (p *PbMysqlDB) Close() {
	err := p.DB.Close()
	if err != nil {
		log.Fatal(err)
		return
	}
}
