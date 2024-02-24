package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"log"
	"os"
	"pbmysql-go/dbproto"
	pkg "pbmysql-go/pkg"
)

func main() {

	file, err := os.Open("db.json")
	defer file.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	pbMySqlTableList := pkg.NewPb2DbTables()
	pbMySqlTableList.CreateMysqlTable(&dbproto.GolangTest{})

	decoder := json.NewDecoder(file)
	jsonConfig := pkg.JsonConfig{}
	err = decoder.Decode(&jsonConfig)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := mysql.NewConnector(pkg.NewMysqlConfig(jsonConfig))
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
