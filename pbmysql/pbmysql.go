package pbmysql_go

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"strconv"
	"strings"
)

func EscapeString(str string, db *sql.DB) string {
	var buffer string
	// Assuming db is a *sql.DB connected to a MySQL database.
	// This is a simplistic way to escape a string for MySQL, proper escaping would depend on the context.
	// For real-world applications, use parameterized queries or prepared statements to avoid SQL injection.
	buffer = fmt.Sprintf("%q", str)
	return buffer
}

type MessageTableInfo struct {
	defaultInstance   proto.Message
	descriptor        *descriptorpb.DescriptorProto
	options           proto.Message
	primaryKeyField   *descriptorpb.DescriptorProto
	autoIncrement     uint64
	fields            map[int]string
	primaryKey        []string
	indexes           []string
	uniqueKeys        []string
	foreignKeys       string
	foreignReferences string
	autoIncreaseKey   string
}

func (m *MessageTableInfo) SetAutoIncrement(autoIncrement uint64) {
	m.autoIncrement = autoIncrement
}

func (m *MessageTableInfo) DefaultInstance() proto.Message {
	return m.defaultInstance
}

// Other methods like GetCreateTableSql, GetInsertSql, etc. should be implemented here.

type Pb2DbTables struct {
	tables map[string]MessageTableInfo
	mysql  *sql.DB
}

// Other methods like GetCreateTableSql, GetInsertSql, etc. should be implemented here.

func (p *Pb2DbTables) AddTable(messageDefaultInstance proto.Message) {
	// Implementation goes here.
}

func (p *Pb2DbTables) SetMysql(mysql *sql.DB) {
	p.mysql = mysql
}

//func EscapeString(str *string, db *sql.DB) {
//buffer := make([]byte, len(*str)*2+1)
//resultSize := db.EscapeString(buffer, []byte(*str))
//*str = string(buffer[:resultSize])
//}

func FillMessageField(message proto.Message, row []string) {

	reflection := proto.MessageReflect(message)
	dscrpt := reflection.Descriptor()
	for i := 0; i < dscrpt.Fields().Len(); i++ {
		fieldDesc := dscrpt.Fields().Get(int(i))
		field := dscrpt.Fields().ByNumber(protowire.Number(i))
		switch fieldDesc.Kind() {
		case protoreflect.Int32Kind:
			typeValue, err := strconv.ParseInt(row[i], 10, 32)
			if nil != err {
				fmt.Println(err)
				continue
			}
			reflection.Set(field, protoreflect.ValueOfInt32(int32(typeValue)))
		case protoreflect.Int64Kind:
			typeValue, err := strconv.ParseInt(row[i], 10, 64)
			if nil != err {
				fmt.Println(err)
				continue
			}
			reflection.Set(field, protoreflect.ValueOfInt64(typeValue))
		case protoreflect.Uint32Kind:
			typeValue, err := strconv.ParseUint(row[i], 10, 32)
			if nil != err {
				fmt.Println(err)
				continue
			}
			reflection.Set(field, protoreflect.ValueOfUint32(uint32(typeValue)))
		case protoreflect.Uint64Kind:
			typeValue, err := strconv.ParseUint(row[i], 10, 64)
			if nil != err {
				fmt.Println(err)
				continue
			}
			reflection.Set(field, protoreflect.ValueOfUint64(typeValue))
		case protoreflect.FloatKind:
			typeValue, err := strconv.ParseFloat(row[i], 32)
			if nil != err {
				fmt.Println(err)
				continue
			}
			reflection.Set(field, protoreflect.ValueOfFloat32(float32(typeValue)))
		case protoreflect.DoubleKind:
			typeValue, err := strconv.ParseFloat(row[i], 64)
			if nil != err {
				fmt.Println(err)
				continue
			}
			reflection.Set(field, protoreflect.ValueOfFloat64(typeValue))
		case protoreflect.StringKind:
			if row[i] == "" {
				typeValue := ""
				reflection.Set(field, protoreflect.ValueOfString(typeValue))
			} else {
				typeValue := row[i]
				reflection.Set(field, protoreflect.ValueOfString(typeValue))
			}
		case protoreflect.BoolKind:
			if row[i] != "" {
				typeValue, err := strconv.ParseBool(row[i])
				if nil != err {
					fmt.Println(err)
					continue
				}
				reflection.Set(field, protoreflect.ValueOfBool(typeValue))
			} else {
				reflection.Set(field, protoreflect.ValueOfBool(false))
			}
		case protoreflect.MessageKind:
			if row[i] != "" {
				subMessage := reflection.Mutable(field).Message()

				proto.Unmarshal([]byte(row[i]), subMessage)
			}
		}
	}
}

var tableNameDescriptor = []string{
	"",
	"int NOT NULL",
	"bigint NOT NULL",
	"int unsigned NOT NULL",
	"bigint unsigned NOT NULL",
	"double NOT NULL DEFAULT '0'",
	"float NOT NULL DEFAULT '0'",
	"bool",
	"int NOT NULL",
	"varchar(256)",
	"Blob",
}

func ConvertFieldValue(message proto.Message, fieldDesc protoreflect.FieldDescriptor, db *sql.DB) string {
	reflection := message.ProtoReflect()
	fieldValue := ""
	switch fieldDesc.Kind() {
	case protoreflect.Int32Kind:
		fieldValue = fmt.Sprintf("%d", reflection.Get(fieldDesc).Int())
	case protoreflect.Uint32Kind:
		fieldValue = fmt.Sprintf("%d", reflection.Get(fieldDesc).Uint())
	case protoreflect.FloatKind:
		fieldValue = fmt.Sprintf("%f", reflection.Get(fieldDesc).Float())
	case protoreflect.StringKind:
		fieldValue = reflection.Get(fieldDesc).String()
	case protoreflect.Int64Kind:
		fieldValue = fmt.Sprintf("%d", reflection.Get(fieldDesc).Int())
	case protoreflect.Uint64Kind:
		fieldValue = fmt.Sprintf("%d", reflection.Get(fieldDesc).Uint())
	case protoreflect.DoubleKind:
		fieldValue = fmt.Sprintf("%f", reflection.Get(fieldDesc).Float())
	case protoreflect.BoolKind:
		fieldValue = fmt.Sprintf("%t", reflection.Get(fieldDesc).Bool())
	case protoreflect.MessageKind:
		if reflection.Has(fieldDesc) {
			subMessage := reflection.Get(fieldDesc).Message()
			data, _ := subMessage.MarshalText()
			fieldValue = string(data)
		}
	}
	//EscapeString(&fieldValue, db)
	return fieldValue
}

type Message2MysqlSql struct {
	tableName         string
	options           *Options
	primaryKey        []string
	indexes           []string
	uniqueKeys        []string
	autoIncreaseKey   string
	foreignKeys       string
	foreignReferences string
	descriptor        protoreflect.MessageDescriptor
	primaryKeyField   protoreflect.FieldDescriptor
}

func NewMessage2MysqlSql(tableName string, options *Options, descriptor protoreflect.MessageDescriptor) *Message2MysqlSql {
	return &Message2MysqlSql{
		tableName:  tableName,
		options:    options,
		descriptor: descriptor,
	}
}

func (m *Message2MysqlSql) GetCreateTableSql() string {
	sql := "CREATE TABLE IF NOT EXISTS " + m.tableName
	if m.options.GetExtension(OptionPrimaryKey) != "" {
		m.primaryKey = strings.Split(m.options.GetExtension(OptionPrimaryKey), ",")
	}
	if m.options.GetExtension(OptionIndex) != "" {
		m.indexes = strings.Split(m.options.GetExtension(OptionIndex), ",")
	}
	if m.options.GetExtension(OptionUniqueKey) != "" {
		m.uniqueKeys = strings.Split(m.options.GetExtension(OptionUniqueKey), ",")
	}
	m.autoIncreaseKey = m.options.GetExtension(OptionAutoIncrementKey)
	m.foreignKeys = m.options.GetExtension(OptionForeignKey)
	m.foreignReferences = m.options.GetExtension(OptionForeignReferences)
	sql += " ("
	needComma := false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		field := m.descriptor.Fields().Get(i)
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += field.Name()
		sql += " "
		sql += tableNameDescriptor[field.Kind()]
		if i == kPrimaryKeyIndex {
			sql += " NOT NULL"
		}
		if field.Name() == m.autoIncreaseKey {
			sql += " AUTO_INCREMENT"
		}
	}
	sql += ", PRIMARY KEY ("
	sql += m.options.GetExtension(OptionPrimaryKey)
	sql += ")"
	if m.foreignKeys != "" && m.foreignReferences != "" {
		sql += ", FOREIGN KEY ("
		sql += m.foreignKeys
		sql += ")"
		sql += " REFERENCES "
		sql += m.foreignReferences
	}
	if len(m.uniqueKeys) > 0 {
		sql += ", UNIQUE KEY ("
		sql += m.options.GetExtension(OptionUniqueKey)
		sql += ")"
	}
	for _, index := range m.indexes {
		sql += ", INDEX ("
		sql += index
		sql += ")"
	}
	sql += ") ENGINE = INNODB"
	if m.autoIncreaseKey != "" {
		sql += " AUTO_INCREMENT=1"
	}
	return sql
}

func (m *Message2MysqlSql) GetAlterTableAddFieldSql() string {
	if m.descriptor.Fields().Len() == len(m.primaryKey) {
		return ""
	}
	sql := "ALTER TABLE " + m.tableName
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		field := m.descriptor.Fields().Get(i)
		if _, ok := m.primaryKey[i]; ok {
			continue
		}
		sql += " ADD COLUMN "
		sql += field.Name()
		sql += " "
		sql += tableNameDescriptor[field.Kind()]
		if i+1 < m.descriptor.Fields().Len() {
			sql += ","
		}
	}
	if m.primaryKeyField == nil {
		return ""
	}
	sql += ";"
	return sql
}

func (m *Message2MysqlSql) GetInsertSql(message proto.Message, db *sql.DB) string {
	sql := "INSERT INTO " + m.tableName
	sql += " ("
	needComma := false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += m.descriptor.Fields().Get(i).Name()
	}
	sql += ") VALUES ("
	needComma = false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		fieldDesc := m.descriptor.Fields().Get(i)
		value := ConvertFieldValue(message, fieldDesc, db)
		sql += "'" + value + "'"
	}
	sql += ")"
	return sql
}

func (m *Message2MysqlSql) GetInsertOnDupUpdateSql(message proto.Message, db *sql.DB) string {
	sql := m.GetInsertSql(message, db)
	sql += " ON DUPLICATE KEY UPDATE "
	sql += m.GetUpdateSet(message, db)
	return sql
}

func (m *Message2MysqlSql) GetInsertOnDupKeyForPrimaryKey(message proto.Message, db *sql.DB) string {
	sql := m.GetInsertSql(message, db)
	sql += " ON DUPLICATE KEY UPDATE "
	sql += " " + m.primaryKeyField.Name()
	value := ConvertFieldValue(message, m.primaryKeyField, db)
	sql += "="
	sql += "'" + value + "';"
	return sql
}

func (m *Message2MysqlSql) GetSelectSql(key, val string) string {
	sql := "SELECT "
	needComma := false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += m.descriptor.Fields().Get(i).Name()
	}
	sql += " FROM "
	sql += m.tableName
	sql += " WHERE "
	sql += key
	sql += " = '"
	sql += val
	sql += "';"
	return sql
}

func (m *Message2MysqlSql) GetSelectSqlWithWhereClause(whereClause string) string {
	sql := "SELECT "
	needComma := false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += m.descriptor.Fields().Get(i).Name()
	}
	sql += " FROM "
	sql += m.tableName
	sql += " WHERE "
	sql += whereClause
	sql += ";"
	return sql
}

func (m *Message2MysqlSql) GetSelectAllSql() string {
	sql := "SELECT "
	needComma := false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += m.descriptor.Fields().Get(i).Name()
	}
	sql += " FROM "
	sql += m.tableName
	return sql
}

func (m *Message2MysqlSql) GetSelectAllSqlWithWhereClause(whereClause string) string {
	sql := "SELECT "
	needComma := false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += m.descriptor.Fields().Get(i).Name()
	}
	sql += " FROM "
	sql += m.tableName
	sql += " WHERE "
	sql += whereClause
	sql += ";"
	return sql
}

func (m *Message2MysqlSql) GetDeleteSql(message proto.Message, db *sql.DB) string {
	sql := "DELETE "
	sql += " FROM "
	sql += m.tableName
	sql += " WHERE "
	sql += m.descriptor.Fields().Get(kPrimaryKeyIndex).Name()
	value := ConvertFieldValue(message, m.primaryKeyField, db)
	sql += " = '"
	sql += value
	sql += "'"
	return sql
}

func (m *Message2MysqlSql) GetDeleteSqlWithWhereClause(whereClause string, db *sql.DB) string {
	sql := "DELETE "
	sql += " FROM "
	sql += m.tableName
	sql += " WHERE "
	sql += whereClause
	return sql
}

func (m *Message2MysqlSql) GetReplaceSql(message proto.Message, db *sql.DB) string {
	sql := "REPLACE INTO " + m.tableName
	sql += " ("
	needComma := false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += m.descriptor.Fields().Get(i).Name()
	}
	sql += ") VALUES ("
	needComma = false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		fieldDesc := m.descriptor.Fields().Get(i)
		value := ConvertFieldValue(message, fieldDesc, db)
		sql += "'" + value + "'"
	}
	sql += ")"
	return sql
}

func (m *Message2MysqlSql) GetUpdateSet(message proto.Message, db *sql.DB) string {
	sql := ""
	needComma := false
	reflection := message.ProtoReflect()
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		field := m.descriptor.Fields().Get(i)
		if reflection.Has(field) {
			if needComma {
				sql += ", "
			} else {
				needComma = true
			}
			sql += " " + field.Name()
			value := ConvertFieldValue(message, field, db)
			sql += "="
			sql += "'" + value + "'"
		}
	}
	return sql
}

func (m *Message2MysqlSql) GetUpdateSql(message proto.Message, db *sql.DB) string {
	sql := "UPDATE " + m.tableName
	needComma := false
	sql += " SET "
	sql += m.GetUpdateSet(message, db)
	sql += " WHERE "
	needComma = false
	for _, primaryKey := range m.primaryKey {
		field := m.descriptor.Fields().ByName(primaryKey)
		if reflection.Has(field) {
			if needComma {
				sql += " AND "
			} else {
				needComma = true
			}
			sql += primaryKey
			value := ConvertFieldValue(message, field, db)
			sql += "='"
			sql += value
			sql += "'"
		}
	}
	return sql
}

func (m *Message2MysqlSql) GetUpdateSqlWithWhereClause(message proto.Message, db *sql.DB, whereClause string) string {
	sql := "UPDATE " + m.tableName
	needComma := false
	sql += " SET "
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += " " + m.descriptor.Fields().Get(i).Name()
		fieldDesc := m.descriptor.Fields().Get(i)
		value := ConvertFieldValue(message, fieldDesc, db)
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

func (m *Message2MysqlSql) GetTruncateSql(message proto.Message) string {
	sql := "Truncate " + message.ProtoReflect().Descriptor().FullName()
	return sql
}

func (m *Message2MysqlSql) GetSelectColumn() string {
	return fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s';", m.tableName)
}

func NewPb2DbTables() *Pb2DbTables {
	return &Pb2DbTables{
		tables: make(map[string]*Message2MysqlSql),
	}
}

func (p *Pb2DbTables) SetAutoIncrement(message proto.Message, autoIncrement uint64) {
	tableName := message.ProtoReflect().Descriptor().FullName()
	if table, ok := p.tables[tableName]; ok {
		table.autoIncreaseKey = autoIncrement
	}
}

func (p *Pb2DbTables) GetCreateTableSql(message proto.Message) string {
	tableName := message.ProtoReflect().Descriptor().FullName()
	if table, ok := p.tables[tableName]; ok {
		return table.GetCreateTableSql()
	}
	return ""
}

func (p *Pb2DbTables) GetAlterTableAddFieldSql(message proto.Message) string {
	tableName := message.ProtoReflect().Descriptor().FullName()
	if table, ok := p.tables[tableName]; ok {
		return table.GetAlterTableAddFieldSql()
	}
	return ""
}

func (p *Pb2DbTables) RegisterTable(tableName string, options *Options, descriptor protoreflect.MessageDescriptor) {
	p.tables[tableName] = NewMessage2MysqlSql(tableName, options, descriptor)
}

// FillMessageField and other helper functions should be implemented here.
