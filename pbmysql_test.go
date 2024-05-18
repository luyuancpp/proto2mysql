package pbmysql_go

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/golang/protobuf/proto"
	"github.com/luyuancpp/dbprotooption"
	"log"
	"os"
	"testing"
)

func GetMysqlConfig() *mysql.Config {
	file, err := os.Open("db.json")
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(file)
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
	pbMySqlDB.AddMysqlTable(&dbprotooption.GolangTest{})

	mysqlConfig := GetMysqlConfig()
	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(conn)
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(db)

	err = pbMySqlDB.OpenDB(db, mysqlConfig.DBName)
	if err != nil {
		log.Fatal(err)
	}

	result, err := db.Exec(pbMySqlDB.GetCreateTableSql(&dbprotooption.GolangTest{}))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(result)
}

func TestAlterTable(t *testing.T) {
	pbMySqlDB := NewPb2DbTables()
	pbMySqlDB.AddMysqlTable(&dbprotooption.GolangTest{})

	mysqlConfig := GetMysqlConfig()
	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(conn)
	defer db.Close()

	pbMySqlDB.OpenDB(db, mysqlConfig.DBName)

	pbMySqlDB.AlterTableAddField(&dbprotooption.GolangTest{})
}

func TestLoadSave(t *testing.T) {
	pbMySqlDB := NewPb2DbTables()
	pbSave := &dbprotooption.GolangTest{
		Id:      1,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &dbprotooption.Player{
			PlayerId: 111,
			Name:     "foo\\0bar,foo\\nbar,foo\\rbar,foo\\Zbar,foo\\\"bar,foo\\\\bar,foo\\'bar",
		},
	}
	pbMySqlDB.AddMysqlTable(pbSave)
	mysqlConfig := GetMysqlConfig()
	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(conn)
	defer db.Close()
	pbMySqlDB.OpenDB(db, mysqlConfig.DBName)

	pbMySqlDB.Save(pbSave)

	pbload := &dbprotooption.GolangTest{}
	pbMySqlDB.LoadOneByKV(pbload, "id", "1")
	if !proto.Equal(pbSave, pbload) {
		log.Fatal("pb not equal")
	}
}

func TestLoadSaveList(t *testing.T) {
	pbMySqlDB := NewPb2DbTables()
	pbSaveList := &dbprotooption.GolangTestList{
		TestList: []*dbprotooption.GolangTest{
			{
				Id:      1,
				GroupId: 1,
				Ip:      "127.0.0.1",
				Port:    3306,
				Player: &dbprotooption.Player{
					PlayerId: 111,
					Name:     "foo\\0bar,foo\\nbar,foo\\rbar,foo\\Zbar,foo\\\"bar,foo\\\\bar,foo\\'bar",
				},
			},
			{
				Id:      2,
				GroupId: 1,
				Ip:      "127.0.0.1",
				Port:    3306,
				Player: &dbprotooption.Player{
					PlayerId: 111,
					Name:     "foo\\0bar,foo\\nbar,foo\\rbar,foo\\Zbar,foo\\\"bar,foo\\\\bar,foo\\'bar",
				},
			},
		},
	}
	pbMySqlDB.AddMysqlTable(&dbprotooption.GolangTest{})
	mysqlConfig := GetMysqlConfig()
	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(conn)
	defer db.Close()
	pbMySqlDB.OpenDB(db, mysqlConfig.DBName)

	pbLoadList := &dbprotooption.GolangTestList{}
	pbMySqlDB.LoadList(pbLoadList)
	if !proto.Equal(pbSaveList, pbLoadList) {
		fmt.Println(pbSaveList.String())
		fmt.Println(pbLoadList.String())
		log.Fatal("pb not equal")
	}
}
