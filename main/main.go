package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
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
		fmt.Println(err)
		return
	}

	conn, err := mysql.NewConnector(pkg.NewMysqlConfig(jsonConfig))
	if err != nil {
		fmt.Println(err)
		return
	}
	db := sql.OpenDB(conn)
	defer db.Close()
	db.Exec(pbMySqlTableList.GetCreateTableSql(&dbproto.GolangTest{}))
}
