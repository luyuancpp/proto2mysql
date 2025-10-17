package protobuf_to_mysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/luyuancpp/dbprotooption"
	"google.golang.org/protobuf/proto"
	"log"
	"os"
	"strconv"
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

	err = pbMySqlDB.UpdateTableField(&dbprotooption.GolangTest{})
	if err != nil {
		return
	}
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

	err = pbMySqlDB.Save(pbSave)
	if err != nil {
		log.Fatal(err)
		return
	}
	err = pbMySqlDB.Save(pbSave1)
	if err != nil {
		log.Fatal(err)
		return
	}

	pbLoad := &dbprotooption.GolangTest{}
	err = pbMySqlDB.FindOneByKV(pbLoad, "id", "1")
	if err != nil {
		log.Fatal(err)
		return
	}
	if !proto.Equal(pbSave, pbLoad) {
		t.Errorf("pb not equal")
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

	err = pbMySqlDB.Save(pbSave)
	if err != nil {
		log.Fatal(err)
		return
	}

	pbLoad := &dbprotooption.GolangTest{}
	err = pbMySqlDB.FindOneByWhereCase(pbLoad, "where id=1")
	if err != nil {
		log.Fatal(err)
		return
	}
	if !proto.Equal(pbSave, pbLoad) {
		log.Fatal("pb not equal")
	}
}

// TestSpecialCharacterEscape 测试特殊字符的转义与还原是否正确
func TestSpecialCharacterEscape(t *testing.T) {
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
		t.Fatalf("无法打开数据库: %v", err)
	}

	// 准备包含各种需要转义的特殊字符的测试数据
	specialChars := "!@#$%^&*()_+{}[]|\\:;\"'<>,.?/`~= \t\n\r\v\f"
	testID := 1001 // 使用独特ID避免与其他测试冲突

	// 保存测试数据
	pbSave := &dbprotooption.GolangTest{
		Id:      uint32(testID),
		GroupId: 100,
		Ip:      "192.168.1.1",
		Port:    3306,
		Player: &dbprotooption.Player{
			PlayerId: uint64(testID),
			Name:     "SpecialCharsTest: " + specialChars,
		},
	}

	err = pbMySqlDB.Save(pbSave)
	if err != nil {
		t.Fatalf("保存包含特殊字符的数据失败: %v", err)
	}

	// 从数据库读取数据
	pbLoad := &dbprotooption.GolangTest{}
	err = pbMySqlDB.FindOneByKV(pbLoad, "id", strconv.FormatUint(uint64(testID), 10))
	if err != nil {
		t.Fatalf("读取包含特殊字符的数据失败: %v", err)
	}

	// 验证数据一致性
	if !proto.Equal(pbSave, pbLoad) {
		t.Error("保存和读取的特殊字符数据不一致")
		t.Logf("预期: %s", pbSave.Player.Name)
		t.Logf("实际: %s", pbLoad.Player.Name)
	}
}

// TestStringWithSpaces 测试包含空格的字符串处理是否正确
func TestStringWithSpaces(t *testing.T) {
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
		t.Fatalf("无法打开数据库: %v", err)
	}

	// 准备包含各种空格的测试数据
	testCases := []struct {
		id       int32
		name     string
		expected string
	}{
		{
			id:       2001,
			name:     "Single space between words",
			expected: "Single space between words",
		},
		{
			id:       2002,
			name:     "  Double  spaces  between  words  ",
			expected: "  Double  spaces  between  words  ",
		},
		{
			id:       2003,
			name:     "Leading space",
			expected: "Leading space",
		},
		{
			id:       2004,
			name:     "Trailing space ",
			expected: "Trailing space ",
		},
		{
			id:       2005,
			name:     "Mixed\tspaces\nand\vother\fwhitespace",
			expected: "Mixed\tspaces\nand\vother\fwhitespace",
		},
	}

	// 保存所有测试数据
	for _, tc := range testCases {
		pbSave := &dbprotooption.GolangTest{
			Id:      uint32(tc.id),
			GroupId: 200,
			Ip:      "192.168.2.1",
			Port:    3306,
			Player: &dbprotooption.Player{
				PlayerId: uint64(tc.id),
				Name:     tc.name,
			},
		}

		err = pbMySqlDB.Save(pbSave)
		if err != nil {
			t.Errorf("保存ID为%d的空格测试数据失败: %v", tc.id, err)
			continue
		}
	}

	// 验证所有测试数据
	for _, tc := range testCases {
		pbLoad := &dbprotooption.GolangTest{}
		err = pbMySqlDB.FindOneByKV(pbLoad, "id", strconv.FormatUint(uint64(tc.id), 10))
		if err != nil {
			t.Errorf("读取ID为%d的空格测试数据失败: %v", tc.id, err)
			continue
		}

		if pbLoad.Player.Name != tc.expected {
			t.Errorf("ID为%d的空格处理不一致", tc.id)
			t.Logf("预期: '%s' (长度: %d)", tc.expected, len(tc.expected))
			t.Logf("实际: '%s' (长度: %d)", pbLoad.Player.Name, len(pbLoad.Player.Name))
		}
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
	err = pbMySqlDB.FindAllByWhereCase(pbLoadList, "where group_id=1")
	if err != nil {
		log.Fatal(err)
		return
	}
	if !proto.Equal(pbSaveList, pbLoadList) {
		fmt.Println(pbSaveList.String())
		fmt.Println(pbLoadList.String())
		t.Error("pb not equal")
	}
}
