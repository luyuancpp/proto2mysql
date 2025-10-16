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
			return "<map_marshal_error>"
		}
		return string(EscapeBytesBackslash(nil, data))
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
			return "<list_marshal_error>"
		}
		return string(EscapeBytesBackslash(nil, data))
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
		b := reflection.Get(fieldDesc).Bytes()
		return string(EscapeBytesBackslash(nil, b))
	case protoreflect.MessageKind:
		if reflection.Has(fieldDesc) {
			subMessage := reflection.Get(fieldDesc).Message()
			data, err := proto.Marshal(subMessage.Interface())
			if err == nil {
				return string(EscapeBytesBackslash(nil, data))
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
			data := UnescapeBytesBackslash([]byte(fieldValue))
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
			data := UnescapeBytesBackslash([]byte(fieldValue))
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
			reflection.Set(fieldDesc, protoreflect.ValueOfBytes(UnescapeBytesBackslash([]byte(row[i]))))
		case protoreflect.MessageKind:
			subMsg := reflection.Mutable(fieldDesc).Message()
			err := proto.Unmarshal(UnescapeBytesBackslash([]byte(row[i])), subMsg.Interface())
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

func EscapeBytesBackslash(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos+1] = '0'
			buf[pos] = '\\'
			pos += 2
		case '\n':
			buf[pos+1] = 'n'
			buf[pos] = '\\'
			pos += 2
		case '\r':
			buf[pos+1] = 'r'
			buf[pos] = '\\'
			pos += 2
		case '\x1a':
			buf[pos+1] = 'Z'
			buf[pos] = '\\'
			pos += 2
		case '\'':
			buf[pos+1] = '\''
			buf[pos] = '\\'
			pos += 2
		case '"':
			buf[pos+1] = '"'
			buf[pos] = '\\'
			pos += 2
		case '\\':
			buf[pos+1] = '\\'
			buf[pos] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

func EscapeStringBackslash(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '\x00':
			buf[pos+1] = '0'
			buf[pos] = '\\'
			pos += 2
		case '\n':
			buf[pos+1] = 'n'
			buf[pos] = '\\'
			pos += 2
		case '\r':
			buf[pos+1] = 'r'
			buf[pos] = '\\'
			pos += 2
		case '\x1a':
			buf[pos+1] = 'Z'
			buf[pos] = '\\'
			pos += 2
		case '\'':
			buf[pos+1] = '\''
			buf[pos] = '\\'
			pos += 2
		case '"':
			buf[pos+1] = '"'
			buf[pos] = '\\'
			pos += 2
		case '\\':
			buf[pos+1] = '\\'
			buf[pos] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

func EscapeBytesQuotes(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		if c == '\'' {
			buf[pos+1] = '\''
			buf[pos] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

func EscapeStringQuotes(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\'' {
			buf[pos+1] = '\''
			buf[pos] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// 反斜杠转义的反向操作（从数据库读取时使用）
func UnescapeBytesBackslash(v []byte) []byte {
	result := make([]byte, 0, len(v))
	i := 0
	for i < len(v) {
		if v[i] == '\\' && i+1 < len(v) {
			switch v[i+1] {
			case '0':
				result = append(result, '\x00')
			case 'n':
				result = append(result, '\n')
			case 'r':
				result = append(result, '\r')
			case 'Z':
				result = append(result, '\x1a')
			case '\'':
				result = append(result, '\'')
			case '"':
				result = append(result, '"')
			case '\\':
				result = append(result, '\\')
			default:
				// 未知转义序列，保留原样
				result = append(result, v[i], v[i+1])
			}
			i += 2
		} else {
			result = append(result, v[i])
			i++
		}
	}
	return result
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

func (m *MessageTable) GetInsertSqlStmt(message proto.Message) string {
	stmt := m.insertSQL + "("
	needComma := false
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		if needComma {
			stmt += ", "
		} else {
			needComma = true
		}
		fieldDesc := m.Descriptor.Fields().Get(i)
		value := SerializeFieldAsString(message, fieldDesc)
		stmt += "'" + value + "'"
	}
	stmt += ")"
	return stmt
}

func (m *MessageTable) GetInsertOnDupUpdateSqlStmt(message proto.Message, db *sql.DB) string {
	stmt := m.GetInsertSqlStmt(message)
	stmt += " ON DUPLICATE KEY UPDATE "
	stmt += m.GetUpdateSetStmt(message)
	return stmt
}

func (m *MessageTable) GetInsertOnDupKeyForPrimaryKeyStmt(message proto.Message, db *sql.DB) string {
	stmt := m.GetInsertSqlStmt(message)
	stmt += " ON DUPLICATE KEY UPDATE "
	stmt += " " + string(m.primaryKeyField.Name())
	value := SerializeFieldAsString(message, m.primaryKeyField)
	stmt += "="
	stmt += "'" + value + "';"
	return stmt
}

func (m *MessageTable) GetSelectSqlByKVWhereStmt(whereType, whereVal string) string {
	stmt := m.getSelectFieldsFromTableSqlStmt()
	stmt += " WHERE "
	stmt += whereType
	stmt += " = '"
	stmt += whereVal
	stmt += "';"
	return stmt
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

// 获取插入数据的 SQL 语句
func (m *MessageTable) GetInsertSqlStmt1(message proto.Message) string {
	stmt := "INSERT INTO " + m.tableName + " (" + m.getFieldsSqlStmt() + ") VALUES ("
	values := []string{}
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		value := SerializeFieldAsString(message, fieldDesc)
		values = append(values, "'"+value+"'")
	}
	stmt += strings.Join(values, ", ") + ")"
	return stmt
}

// 获取更新数据的 SQL 语句
func (m *MessageTable) GetUpdateSqlStmt(message proto.Message) string {
	stmt := "UPDATE " + m.tableName + " SET "
	setClause := []string{}

	// 为每个字段生成 SET 子句
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		value := SerializeFieldAsString(message, fieldDesc)
		setClause = append(setClause, string(fieldDesc.Name())+"='"+value+"'")
	}

	stmt += strings.Join(setClause, ", ") + " WHERE " + m.primaryKey[0] + " = ?;"
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

func (m *MessageTable) GetDeleteSql(message proto.Message, db *sql.DB) string {
	stmt := "DELETE  FROM "
	stmt += m.tableName
	stmt += " WHERE "
	stmt += string(m.Descriptor.Fields().Get(kPrimaryKeyIndex).Name())
	value := SerializeFieldAsString(message, m.primaryKeyField)
	stmt += " = '"
	stmt += value
	stmt += "'"
	return stmt
}

func (m *MessageTable) GetDeleteSqlWithWhereClause(whereClause string) string {
	stmt := "DELETE FROM "
	stmt += m.tableName
	stmt += " WHERE "
	stmt += whereClause
	return stmt
}

func (m *MessageTable) GetReplaceIntoSql(message proto.Message) string {
	sql := m.replaceSQL
	needComma := false
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		fieldDesc := m.Descriptor.Fields().Get(i)
		value := SerializeFieldAsString(message, fieldDesc)
		sql += "'" + value + "'"
	}
	sql += ")"
	return sql
}

func (m *MessageTable) GetUpdateSetStmt(message proto.Message) string {
	stmt := ""
	needComma := false
	reflection := message.ProtoReflect()
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		if reflection.Has(field) {
			if needComma {
				stmt += ", "
			} else {
				needComma = true
			}
			stmt += " " + string(field.Name())
			value := SerializeFieldAsString(message, field)
			stmt += "="
			stmt += "'" + value + "'"
		}
	}
	return stmt
}

func (m *MessageTable) GetUpdateSql(message proto.Message, db *sql.DB) string {
	stmt := "UPDATE " + m.tableName
	stmt += " SET "
	stmt += m.GetUpdateSetStmt(message)
	stmt += " WHERE "
	needComma := false
	for _, primaryKey := range m.primaryKey {
		field := m.Descriptor.Fields().ByName(protoreflect.Name(primaryKey))
		if nil != field {
			if needComma {
				stmt += " AND "
			} else {
				needComma = true
			}
			stmt += primaryKey
			value := SerializeFieldAsString(message, field)
			stmt += "='"
			stmt += value
			stmt += "'"
		}
	}
	return stmt
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

func (m *MessageTable) GetUpdateSqlWithWhereClause(message proto.Message, whereClause string) string {
	sql := "UPDATE " + m.tableName
	needComma := false
	sql += " SET "
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += " " + string(m.Descriptor.Fields().Get(i).Name())
		fieldDesc := m.Descriptor.Fields().Get(i)
		value := SerializeFieldAsString(message, fieldDesc)
		sql += "="
		sql += "'" + value + "'"
	}
	if whereClause != "" {
		sql += " WHERE "
		sql += whereClause
	} else {
		sql = ""
	}
	return sql
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
		return fmt.Errorf("table not found")
	}
	_, err := p.DB.Exec(table.GetReplaceIntoSql(message))
	if err != nil {
		return err
	}

	return nil
}

func (p *PbMysqlDB) LoadOneByKV(message proto.Message, whereType string, whereValue string) error {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return fmt.Errorf("table not found")
	}
	rows, err := p.DB.Query(table.GetSelectSqlByKVWhereStmt(whereType, whereValue))
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

func (p *PbMysqlDB) LoadOneByWhereCase(message proto.Message, whereCase string) error {
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

func (p *PbMysqlDB) LoadList(message proto.Message) error {
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

func (p *PbMysqlDB) LoadListByWhereCase(message proto.Message, whereCase string) error {
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
