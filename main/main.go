package main

import (
	"encoding/json"
	"fmt"
	"os"
	"pbmysql-go/dbproto"
	pbmysql_go "pbmysql-go/pkg"
)

func main() {

	file, err := os.Open("db.json")
	defer file.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	pbmsyqltablelist := pbmysql_go.NewPb2DbTables()
	pbmsyqltablelist.CreateMysqlTable(&dbproto.GolangTest{})

	decoder := json.NewDecoder(file)
	jsonCofig := pbmysql_go.JsonConfig{}
	err = decoder.Decode(&jsonCofig)
	if err != nil {
		fmt.Println(err)
		return
	}

}
