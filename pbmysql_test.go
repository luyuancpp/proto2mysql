package pbmysql_go

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/luyuancpp/dbprotooption"
	"google.golang.org/protobuf/proto"
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
	pbMySqlDB := NewPbMysqlDB()
	pbMySqlDB.RegisterTable(&dbprotooption.GolangTest{})

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
	pbMySqlDB := NewPbMysqlDB()
	pbMySqlDB.RegisterTable(&dbprotooption.GolangTest{})

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
		return
	}

	pbMySqlDB.UpdateTableField(&dbprotooption.GolangTest{})
}

func TestLoadSave(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
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
	pbSave1 := &dbprotooption.GolangTest{
		Id:      2,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &dbprotooption.Player{
			PlayerId: 111,
			Name:     "foo\\0bar,foo\\nbar,foo\\rbar,foo\\Zbar,foo\\\"bar,foo\\\\bar,foo\\'bar",
		},
	}
	pbMySqlDB.RegisterTable(pbSave)
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
		return
	}

	pbMySqlDB.Save(pbSave)
	pbMySqlDB.Save(pbSave1)

	pbLoad := &dbprotooption.GolangTest{}
	pbMySqlDB.LoadOneByKV(pbLoad, "id", "1")
	if !proto.Equal(pbSave, pbLoad) {
		log.Fatal("pb not equal")
	}
}

func TestLoadByWhereCase(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
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
	pbMySqlDB.RegisterTable(pbSave)
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
		return
	}

	pbMySqlDB.Save(pbSave)

	pbLoad := &dbprotooption.GolangTest{}
	pbMySqlDB.LoadOneByWhereCase(pbLoad, "where id=1")
	if !proto.Equal(pbSave, pbLoad) {
		log.Fatal("pb not equal")
	}
}

func TestLoadSaveList(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
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
	pbMySqlDB.RegisterTable(&dbprotooption.GolangTest{})
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
		return
	}

	pbLoadList := &dbprotooption.GolangTestList{}
	pbMySqlDB.LoadList(pbLoadList)
	if !proto.Equal(pbSaveList, pbLoadList) {
		fmt.Println(pbSaveList.String())
		fmt.Println(pbLoadList.String())
		log.Fatal("pb not equal")
	}
}

func TestLoadSaveListWhereCase(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
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
	pbMySqlDB.RegisterTable(&dbprotooption.GolangTest{})
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
		return
	}

	pbLoadList := &dbprotooption.GolangTestList{}
	pbMySqlDB.LoadListByWhereCase(pbLoadList, "where group_id=1")
	if !proto.Equal(pbSaveList, pbLoadList) {
		fmt.Println(pbSaveList.String())
		fmt.Println(pbLoadList.String())
		log.Fatal("pb not equal")
	}
}
