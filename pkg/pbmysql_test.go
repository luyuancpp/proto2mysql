package pkb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"log"
	"os"
	"pbmysql-go/dbproto"
	"testing"
)

func GetMysqlConfig() *mysql.Config {
	file, err := os.Open("db.json")
	defer file.Close()
	if err != nil {
		fmt.Println(err)
		return nil
	}
	decoder := json.NewDecoder(file)
	jsonConfig := JsonConfig{}
	err = decoder.Decode(&jsonConfig)
	if err != nil {
		log.Fatal(err)
	}
	return NewMysqlConfig(jsonConfig)
}

func TestCreateTable(t *testing.T) {
	pbMySqlDB := NewPb2DbTables()
	pbMySqlDB.CreateMysqlTable(&dbproto.GolangTest{})

	mysqlConfig := GetMysqlConfig()
	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(conn)
	defer db.Close()
	pbMySqlDB.SetDB(db, mysqlConfig.DBName)
	pbMySqlDB.UseDB()
	result, err := db.Exec(pbMySqlDB.GetCreateTableSql(&dbproto.GolangTest{}))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(result)
}

func TestAlterTable(t *testing.T) {
	pbMySqlDB := NewPb2DbTables()
	pbMySqlDB.CreateMysqlTable(&dbproto.GolangTest{})

	mysqlConfig := GetMysqlConfig()
	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(conn)
	pbMySqlDB.SetDB(db, mysqlConfig.DBName)
	pbMySqlDB.UseDB()

	defer db.Close()

	pbMySqlDB.AlterTableAddField(&dbproto.GolangTest{})
}
