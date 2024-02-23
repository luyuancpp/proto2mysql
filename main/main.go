package main

import (
	"pbmysql-go/dbproto"
	pbmysql_go "pbmysql-go/pbmysql"
)

func main() {
	pbmsyqltablelist := pbmysql_go.NewPb2DbTables()
	pbmsyqltablelist.CreateMysqlTable(&dbproto.GolangTest{})
}
