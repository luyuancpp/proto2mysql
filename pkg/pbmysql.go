package pkg

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
	"pbmysql-go/dbproto"
	"strconv"
	"strings"
)

const (
	kPrimaryKeyIndex = 0
)

type MessageTableInfo struct {
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
	selectAllSqlStmt             string
	selectFieldsFromTableSqlStmt string
	fieldsSqlStmt                string
}

func (m *MessageTableInfo) SetAutoIncrement(autoIncrement uint64) {
	m.autoIncrement = autoIncrement
}

func (m *MessageTableInfo) DefaultInstance() proto.Message {
	return m.defaultInstance
}

type PbMysqlDB struct {
	Tables map[string]*MessageTableInfo
	DB     *sql.DB
	DBName string
}

func (p *PbMysqlDB) SetDB(db *sql.DB, dbname string) {
	p.DB = db
	p.DBName = dbname
}

func (p *PbMysqlDB) UseDB() {
	_, err := p.DB.Query("USE " + p.DBName)
	if err != nil {
		fmt.Println(err)

	}
}

func SerializeFieldAsString(message proto.Message, fieldDesc protoreflect.FieldDescriptor, db *sql.DB) string {
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
			var buf []byte
			fieldValue = string(EscapeBytesBackslash(buf, data))
		}
	}
	return fieldValue
}

func ParseFromString(message proto.Message, row []string) {
	reflection := proto.MessageReflect(message)
	dscrpt := reflection.Descriptor()
	for i := 0; i < dscrpt.Fields().Len(); i++ {
		fieldDesc := dscrpt.Fields().Get(int(i))
		field := dscrpt.Fields().ByNumber(protowire.Number(i + 1))
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

func (m *MessageTableInfo) GetCreateTableSqlStmt() string {
	sql := "CREATE TABLE IF NOT EXISTS " + m.tableName

	if m.options.Has(dbproto.E_OptionPrimaryKey.TypeDescriptor()) {
		v := m.options.Get(dbproto.E_OptionPrimaryKey.TypeDescriptor())
		m.primaryKey = strings.Split(v.String(), ",")
	}
	if m.options.Has(dbproto.E_OptionIndex.TypeDescriptor()) {
		v := m.options.Get(dbproto.E_OptionPrimaryKey.TypeDescriptor())
		m.indexes = strings.Split(v.String(), ",")
	}
	if m.options.Has(dbproto.E_OptionUniqueKey.TypeDescriptor()) {
		m.uniqueKeys = m.options.Get(dbproto.E_OptionUniqueKey.TypeDescriptor()).String()
	}
	m.autoIncreaseKey = m.options.Get(dbproto.E_OptionAutoIncrementKey.TypeDescriptor()).String()
	sql += " ("
	needComma := false
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += string(field.Name())
		sql += " "
		sql += MysqlFieldDescriptorType[field.Kind()]
		if i == kPrimaryKeyIndex {
			sql += " NOT NULL"
		}
		if string(field.Name()) == m.autoIncreaseKey {
			sql += " AUTO_INCREMENT"
		}
	}
	sql += ", PRIMARY KEY ("
	sql += m.primaryKey[kPrimaryKeyIndex]
	sql += ")"

	if len(m.uniqueKeys) > 0 {
		sql += ", UNIQUE KEY ("
		sql += m.uniqueKeys
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

func (m *MessageTableInfo) GetAlterTableAddFieldSqlStmt() string {
	if m.Descriptor.Fields().Len() == len(m.primaryKey) {
		return ""
	}
	sql := "ALTER TABLE " + m.tableName
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		sqlFieldName, ok := m.fields[i]
		fieldName := string(field.Name())
		if ok && sqlFieldName == fieldName {
			continue
		}
		sql += " ADD COLUMN "
		sql += string(field.Name())
		sql += " "
		sql += MysqlFieldDescriptorType[field.Kind()]
		if i+1 < m.Descriptor.Fields().Len() {
			sql += ","
		}
	}
	sql += ";"
	return sql
}

func (m *MessageTableInfo) GetInsertSqlStmt(message proto.Message, db *sql.DB) string {
	sql := "INSERT INTO " + m.tableName
	sql += " ("
	needComma := false
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += string(m.Descriptor.Fields().Get(i).Name())
	}
	sql += ") VALUES ("
	needComma = false
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		fieldDesc := m.Descriptor.Fields().Get(i)
		value := SerializeFieldAsString(message, fieldDesc, db)
		sql += "'" + value + "'"
	}
	sql += ")"
	return sql
}

func (m *MessageTableInfo) GetInsertOnDupUpdateSqlStmt(message proto.Message, db *sql.DB) string {
	sql := m.GetInsertSqlStmt(message, db)
	sql += " ON DUPLICATE KEY UPDATE "
	sql += m.GetUpdateSetStmt(message, db)
	return sql
}

func (m *MessageTableInfo) GetInsertOnDupKeyForPrimaryKeyStmt(message proto.Message, db *sql.DB) string {
	sql := m.GetInsertSqlStmt(message, db)
	sql += " ON DUPLICATE KEY UPDATE "
	sql += " " + string(m.primaryKeyField.Name())
	value := SerializeFieldAsString(message, m.primaryKeyField, db)
	sql += "="
	sql += "'" + value + "';"
	return sql
}

func (m *MessageTableInfo) GetSelectSqlByKVWhereStmt(whereType, whereVal string) string {
	sql := m.getSelectFieldsFromTableSqlStmt()
	sql += " WHERE "
	sql += whereType
	sql += " = '"
	sql += whereVal
	sql += "';"
	return sql
}

func (m *MessageTableInfo) GetSelectSqlStmt() string {
	if len(m.selectAllSqlStmt) > 0 {
		return m.selectAllSqlStmt
	}
	m.selectAllSqlStmt += m.getSelectFieldsFromTableSqlStmt() + ";"
	return m.selectAllSqlStmt
}

func (m *MessageTableInfo) getFieldsSqlStmt() string {
	if len(m.fieldsSqlStmt) > 0 {
		return m.fieldsSqlStmt
	}
	needComma := false
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		if needComma {
			m.fieldsSqlStmt += ", "
		} else {
			needComma = true
		}
		m.fieldsSqlStmt += string(m.Descriptor.Fields().Get(i).Name())
	}
	return m.fieldsSqlStmt
}

func (m *MessageTableInfo) getSelectFieldsFromTableSqlStmt() string {
	if len(m.selectFieldsFromTableSqlStmt) > 0 {
		return m.selectFieldsFromTableSqlStmt
	}
	m.selectFieldsFromTableSqlStmt = "SELECT "
	m.selectFieldsFromTableSqlStmt += m.getFieldsSqlStmt()
	m.selectFieldsFromTableSqlStmt += " FROM "
	m.selectFieldsFromTableSqlStmt += m.tableName
	return m.selectFieldsFromTableSqlStmt
}

func (m *MessageTableInfo) GetSelectSqlWithWhereClause(whereClause string) string {
	sql := m.getSelectFieldsFromTableSqlStmt()
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
	sql += string(m.Descriptor.Fields().Get(kPrimaryKeyIndex).Name())
	value := SerializeFieldAsString(message, m.primaryKeyField, db)
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
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += string(m.Descriptor.Fields().Get(i).Name())
	}
	sql += ") VALUES ("
	needComma = false
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		fieldDesc := m.Descriptor.Fields().Get(i)
		value := SerializeFieldAsString(message, fieldDesc, db)
		sql += "'" + value + "'"
	}
	sql += ")"
	return sql
}

func (m *MessageTableInfo) GetUpdateSetStmt(message proto.Message, db *sql.DB) string {
	sql := ""
	needComma := false
	reflection := proto.MessageReflect(message)
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		if reflection.Has(field) {
			if needComma {
				sql += ", "
			} else {
				needComma = true
			}
			sql += " " + string(field.Name())
			value := SerializeFieldAsString(message, field, db)
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
	sql += m.GetUpdateSetStmt(message, db)
	sql += " WHERE "
	needComma = false
	for _, primaryKey := range m.primaryKey {
		field := m.Descriptor.Fields().ByName(protoreflect.Name(primaryKey))
		if nil != field {
			if needComma {
				sql += " AND "
			} else {
				needComma = true
			}
			sql += primaryKey
			value := SerializeFieldAsString(message, field, db)
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
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		if needComma {
			sql += ", "
		} else {
			needComma = true
		}
		sql += " " + string(m.Descriptor.Fields().Get(i).Name())
		fieldDesc := m.Descriptor.Fields().Get(i)
		value := SerializeFieldAsString(message, fieldDesc, db)
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

func NewPb2DbTables() *PbMysqlDB {
	return &PbMysqlDB{
		Tables: make(map[string]*MessageTableInfo),
	}
}

func GetTableName(m proto.Message) string {
	reflection := proto.MessageReflect(m)
	return string(reflection.Descriptor().FullName())
}

func GetDescriptor(m proto.Message) protoreflect.MessageDescriptor {
	reflection := proto.MessageReflect(m)
	return reflection.Descriptor()
}

func (p *PbMysqlDB) GetCreateTableSql(message proto.Message) string {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		return ""
	}
	return table.GetCreateTableSqlStmt()
}

func (p *PbMysqlDB) AlterTableAddField(message proto.Message) {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		fmt.Println("table not found")
		return
	}
	sqlStmt := fmt.Sprintf("SELECT COLUMN_NAME,ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s';",
		p.DBName,
		table.tableName)

	rows, err := p.DB.Query(sqlStmt)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()

	fieldIndex := 0
	var fieldName string

	for rows.Next() {
		err = rows.Scan(&fieldName, &fieldIndex)
		if err != nil {
			fmt.Println(err)
			return
		}
		table.fields[fieldIndex-1] = fieldName
	}
	p.DB.Exec(table.GetAlterTableAddFieldSqlStmt())
}

func (p *PbMysqlDB) Save(message proto.Message) {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		fmt.Println("table not found")
		return
	}
	_, err := p.DB.Exec(table.GetInsertOnDupUpdateSqlStmt(message, p.DB))
	if err != nil {
		fmt.Println(err)
		return
	}
}

func (p *PbMysqlDB) LoadOneByKV(message proto.Message, whereType string, whereValue string) {
	table, ok := p.Tables[GetTableName(message)]
	if !ok {
		fmt.Println("table not found")
		return
	}
	rows, err := p.DB.Query(table.GetSelectSqlByKVWhereStmt(whereType, whereValue))
	if err != nil {
		fmt.Println(err)
		return
	}
	columns, err := rows.Columns()
	if err != nil {
		fmt.Println(err)
		return
	}
	vals := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k, _ := range vals {
		scans[k] = &vals[k]
	}

	for rows.Next() {
		rows.Scan(scans...)
		i := 0
		result := make([]string, len(columns))
		for _, v := range vals {
			result[i] = string(v)
			i++
		}
		ParseFromString(message, result)
	}
}

func (p *PbMysqlDB) LoadList(message proto.Message) {
	reflectionParent := proto.MessageReflect(message)
	md := reflectionParent.Descriptor()
	fds := md.Fields()
	listField := fds.Get(0)
	name := string(listField.Message().Name())
	table, ok := p.Tables[name]
	if !ok {
		fmt.Println("table not found")
		return
	}
	rows, err := p.DB.Query(table.GetSelectSqlStmt())
	if err != nil {
		fmt.Println(err)
		return
	}
	columns, err := rows.Columns()
	if err != nil {
		fmt.Println(err)
		return
	}
	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for k, _ := range values {
		scans[k] = &values[k]
	}
	lv := reflectionParent.Mutable(listField).List()
	for rows.Next() {
		rows.Scan(scans...)
		i := 0
		result := make([]string, len(columns))
		for _, v := range values {
			result[i] = string(v)
			i++
		}
		ve := lv.NewElement()
		ParseFromString(proto.MessageV1(ve.Message()), result)
		lv.Append(ve)
	}
}

func (p *PbMysqlDB) AddMysqlTable(m proto.Message) {
	p.Tables[GetTableName(m)] = &MessageTableInfo{
		tableName:       GetTableName(m),
		defaultInstance: m,
		Descriptor:      GetDescriptor(m),
		options:         GetDescriptor(m).Options().ProtoReflect(),
		fields:          make(map[int]string)}
}
