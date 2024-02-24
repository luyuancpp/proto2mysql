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

func TestCreateTable(t *testing.T) {
	file, err := os.Open("db.json")
	defer file.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	pbMySqlTableList := NewPb2DbTables()
	pbMySqlTableList.CreateMysqlTable(&dbproto.GolangTest{})

	decoder := json.NewDecoder(file)
	jsonConfig := JsonConfig{}
	err = decoder.Decode(&jsonConfig)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := mysql.NewConnector(NewMysqlConfig(jsonConfig))
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(conn)
	defer db.Close()
	result, err := db.Exec(pbMySqlTableList.GetCreateTableSql(&dbproto.GolangTest{}))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(result)
}

func TestAlterTable(t *testing.T) {
	file, err := os.Open("db.json")
	defer file.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	pbMySqlTableList := NewPb2DbTables()
	pbMySqlTableList.CreateMysqlTable(&dbproto.GolangTest{})

	decoder := json.NewDecoder(file)
	jsonConfig := JsonConfig{}
	err = decoder.Decode(&jsonConfig)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := mysql.NewConnector(NewMysqlConfig(jsonConfig))
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(conn)
	defer db.Close()
	result, err := db.Exec(pbMySqlTableList.GetAlterTableAddFieldSql(&dbproto.GolangTest{}))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(result)
}
