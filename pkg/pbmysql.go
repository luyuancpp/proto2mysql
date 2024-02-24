package pkb

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"pbmysql-go/dbproto"
	"strconv"
	"strings"
)

const (
	kPrimaryKeyIndex = 0
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
	tableName         string
	defaultInstance   proto.Message
	options           descriptorpb.MessageOptions
	primaryKeyField   protoreflect.FieldDescriptor
	autoIncrement     uint64
	fields            map[int]string
	primaryKey        []string
	indexes           []string
	uniqueKeys        []string
	foreignKeys       string
	foreignReferences string
	autoIncreaseKey   string
	descriptor        protoreflect.MessageDescriptor
}

func (m *MessageTableInfo) SetAutoIncrement(autoIncrement uint64) {
	m.autoIncrement = autoIncrement
}

func (m *MessageTableInfo) DefaultInstance() proto.Message {
	return m.defaultInstance
}

// Other methods like GetCreateTableSql, GetInsertSql, etc. should be implemented here.

type Pb2DbTables struct {
	tables map[string]*MessageTableInfo
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
				proto.Unmarshal([]byte(row[i]), proto.MessageV1(subMessage))
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
	reflection := proto.MessageReflect(message)
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
			data, _ := proto.Marshal(proto.MessageV1(subMessage))
			fieldValue = string(data)
		}
	}
	//EscapeString(&fieldValue, db)
	return fieldValue
}

func (m *MessageTableInfo) GetCreateTableSql() string {
	sql := "CREATE TABLE IF NOT EXISTS " + m.tableName

	//descriptorpb.MessageOptions.ProtoReflect.Descriptor.ExtensionRanges
	primaryKeyExtName := dbproto.E_OptionPrimaryKey.TypeDescriptor().Name()
	primaryKeyExtField := m.descriptor.Extensions().ByName(primaryKeyExtName)
	if primaryKeyExtField != nil {
		m.primaryKey = strings.Split(string(primaryKeyExtField.FullName()), ",")
	}
	optionIndexKeyExtName := dbproto.E_OptionIndex.TypeDescriptor().Name()
	optionIndexKeyExtField := m.descriptor.Extensions().ByName(optionIndexKeyExtName)
	if optionIndexKeyExtField != nil {
		m.indexes = strings.Split(string(optionIndexKeyExtField.FullName()), ",")
	}
	optionUniqueKeyExtName := dbproto.E_OptionUniqueKey.TypeDescriptor().Name()
	optionUniqueKeyExtField := m.descriptor.Extensions().ByName(optionUniqueKeyExtName)
	if optionUniqueKeyExtField != nil {
		m.uniqueKeys = strings.Split(string(optionUniqueKeyExtField.FullName()), ",")
	}
	m.autoIncreaseKey = string(m.descriptor.Extensions().ByName(dbproto.E_OptionAutoIncrementKey.TypeDescriptor().Name()).FullName())

	m.foreignKeys = string(m.descriptor.Extensions().ByName(dbproto.E_OptionForeignKey.TypeDescriptor().Name()).FullName())
	m.foreignReferences = string(m.descriptor.Extensions().ByName(dbproto.E_OptionForeignReferences.TypeDescriptor().Name()).FullName())
	sql += " ("
	needComma := false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		field := m.descriptor.Fields().Get(i)
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += string(field.Name())
		sql += " "
		sql += tableNameDescriptor[field.Kind()]
		if i == kPrimaryKeyIndex {
			sql += " NOT NULL"
		}
		if string(field.Name()) == m.autoIncreaseKey {
			sql += " AUTO_INCREMENT"
		}
	}
	sql += ", PRIMARY KEY ("
	sql += string(primaryKeyExtField.Name())
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
		sql += string(optionUniqueKeyExtField.Name())
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

func (m *MessageTableInfo) GetAlterTableAddFieldSql() string {
	if m.descriptor.Fields().Len() == len(m.primaryKey) {
		return ""
	}
	sql := "ALTER TABLE " + m.tableName
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		field := m.descriptor.Fields().Get(i)
		if m.primaryKey[i] != "" {
			continue
		}
		sql += " ADD COLUMN "
		sql += string(field.Name())
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

func (m *MessageTableInfo) GetInsertSql(message proto.Message, db *sql.DB) string {
	sql := "INSERT INTO " + m.tableName
	sql += " ("
	needComma := false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += string(m.descriptor.Fields().Get(i).Name())
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

func (m *MessageTableInfo) GetInsertOnDupUpdateSql(message proto.Message, db *sql.DB) string {
	sql := m.GetInsertSql(message, db)
	sql += " ON DUPLICATE KEY UPDATE "
	sql += m.GetUpdateSet(message, db)
	return sql
}

func (m *MessageTableInfo) GetInsertOnDupKeyForPrimaryKey(message proto.Message, db *sql.DB) string {
	sql := m.GetInsertSql(message, db)
	sql += " ON DUPLICATE KEY UPDATE "
	sql += " " + string(m.primaryKeyField.Name())
	value := ConvertFieldValue(message, m.primaryKeyField, db)
	sql += "="
	sql += "'" + value + "';"
	return sql
}

func (m *MessageTableInfo) GetSelectSql(key, val string) string {
	sql := "SELECT "
	needComma := false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += string(m.descriptor.Fields().Get(i).Name())
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

func (m *MessageTableInfo) GetSelectSqlWithWhereClause(whereClause string) string {
	sql := "SELECT "
	needComma := false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += string(m.descriptor.Fields().Get(i).Name())
	}
	sql += " FROM "
	sql += m.tableName
	sql += " WHERE "
	sql += whereClause
	sql += ";"
	return sql
}

func (m *MessageTableInfo) GetSelectAllSql() string {
	sql := "SELECT "
	needComma := false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += string(m.descriptor.Fields().Get(i).Name())
	}
	sql += " FROM "
	sql += m.tableName
	return sql
}

func (m *MessageTableInfo) GetSelectAllSqlWithWhereClause(whereClause string) string {
	sql := "SELECT "
	needComma := false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += string(m.descriptor.Fields().Get(i).Name())
	}
	sql += " FROM "
	sql += m.tableName
	sql += " WHERE "
	sql += whereClause
	sql += ";"
	return sql
}

func (m *MessageTableInfo) GetDeleteSql(message proto.Message, db *sql.DB) string {
	sql := "DELETE "
	sql += " FROM "
	sql += m.tableName
	sql += " WHERE "
	sql += string(m.descriptor.Fields().Get(kPrimaryKeyIndex).Name())
	value := ConvertFieldValue(message, m.primaryKeyField, db)
	sql += " = '"
	sql += value
	sql += "'"
	return sql
}

func (m *MessageTableInfo) GetDeleteSqlWithWhereClause(whereClause string, db *sql.DB) string {
	sql := "DELETE "
	sql += " FROM "
	sql += m.tableName
	sql += " WHERE "
	sql += whereClause
	return sql
}

func (m *MessageTableInfo) GetReplaceSql(message proto.Message, db *sql.DB) string {
	sql := "REPLACE INTO " + m.tableName
	sql += " ("
	needComma := false
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += string(m.descriptor.Fields().Get(i).Name())
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

func (m *MessageTableInfo) GetUpdateSet(message proto.Message, db *sql.DB) string {
	sql := ""
	needComma := false
	reflection := proto.MessageReflect(message)
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		field := m.descriptor.Fields().Get(i)
		if reflection.Has(field) {
			if needComma {
				sql += ", "
			} else {
				needComma = true
			}
			sql += " " + string(field.Name())
			value := ConvertFieldValue(message, field, db)
			sql += "="
			sql += "'" + value + "'"
		}
	}
	return sql
}

func (m *MessageTableInfo) GetUpdateSql(message proto.Message, db *sql.DB) string {
	sql := "UPDATE " + m.tableName
	needComma := false
	sql += " SET "
	sql += m.GetUpdateSet(message, db)
	sql += " WHERE "
	needComma = false
	for _, primaryKey := range m.primaryKey {
		field := m.descriptor.Fields().ByName(protoreflect.Name(primaryKey))
		if nil != field {
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

func (m *MessageTableInfo) GetUpdateSqlWithWhereClause(message proto.Message, db *sql.DB, whereClause string) string {
	sql := "UPDATE " + m.tableName
	needComma := false
	sql += " SET "
	for i := 0; i < m.descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += " " + string(m.descriptor.Fields().Get(i).Name())
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

func (m *MessageTableInfo) GetTruncateSql(message proto.Message) string {
	reflection := proto.MessageReflect(message)
	return "Truncate " + string(reflection.Descriptor().FullName())
}

func (m *MessageTableInfo) GetSelectColumn() string {
	return fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s';", m.tableName)
}

func NewPb2DbTables() *Pb2DbTables {
	return &Pb2DbTables{
		tables: make(map[string]*MessageTableInfo),
	}
}

func GetTableName(m proto.Message) string {
	reflection := proto.MessageReflect(m)
	return string(reflection.Descriptor().FullName())
}

func (p *Pb2DbTables) GetCreateTableSql(message proto.Message) string {
	if table, ok := p.tables[GetTableName(message)]; ok {
		return table.GetCreateTableSql()
	}
	return ""
}

func (p *Pb2DbTables) GetAlterTableAddFieldSql(message proto.Message) string {
	if table, ok := p.tables[GetTableName(message)]; ok {
		return table.GetAlterTableAddFieldSql()
	}
	return ""
}

func (p *Pb2DbTables) CreateMysqlTable(m proto.Message) {
	p.tables[GetTableName(m)] = &MessageTableInfo{defaultInstance: m}
}

// FillMessageField and other helper functions should be implemented here.
