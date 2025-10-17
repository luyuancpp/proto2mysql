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
	"sort"
	"strconv"
	"testing"
)

// GetMysqlConfig 读取db.json配置
func GetMysqlConfig() *mysql.Config {
	file, err := os.Open("db.json")
	defer func(file *os.File) {
		if file != nil {
			if err := file.Close(); err != nil {
				fmt.Printf("关闭db.json失败: %v\n", err)
			}
		}
	}(file)
	if err != nil {
		fmt.Printf("打开db.json失败: %v\n", err)
		return nil
	}
	decoder := json.NewDecoder(file)
	jsonConfig := JsonConfig{}
	if err := decoder.Decode(&jsonConfig); err != nil {
		log.Fatalf("解析db.json失败: %v", err)
	}
	return NewMysqlConfig(jsonConfig)
}

// TestCreateTable 测试创建表
func TestCreateTable(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
	testTable := &dbprotooption.GolangTest{}
	pbMySqlDB.RegisterTable(testTable)

	mysqlConfig := GetMysqlConfig()
	if mysqlConfig == nil {
		t.Fatal("获取MySQL配置失败，请检查db.json文件")
	}
	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		t.Fatalf("创建MySQL连接器失败: %v", err)
	}
	db := sql.OpenDB(conn)
	defer func(db *sql.DB) {
		if err := db.Close(); err != nil {
			t.Logf("关闭数据库失败: %v", err)
		}
	}(db)

	if err := db.Ping(); err != nil {
		t.Fatalf("数据库连接失败: %v", err)
	}

	if err := pbMySqlDB.OpenDB(db, mysqlConfig.DBName); err != nil {
		t.Fatalf("切换数据库失败: %v", err)
	}

	createSQL := pbMySqlDB.GetCreateTableSQL(testTable)
	if createSQL == "" {
		t.Fatal("生成创建表SQL失败")
	}
	if _, err := db.Exec(createSQL); err != nil {
		t.Fatalf("执行创建表SQL失败: %v, SQL: %s", err, createSQL)
	}
	t.Log("创建表成功")
}

// TestAlterTable 测试修改表字段
func TestAlterTable(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
	testTable := &dbprotooption.GolangTest{}
	pbMySqlDB.RegisterTable(testTable)

	mysqlConfig := GetMysqlConfig()
	if mysqlConfig == nil {
		t.Fatal("获取MySQL配置失败")
	}
	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		t.Fatalf("创建连接器失败: %v", err)
	}
	db := sql.OpenDB(conn)
	defer func(db *sql.DB) {
		if err := db.Close(); err != nil {
			t.Logf("关闭数据库失败: %v", err)
		}
	}(db)

	if err := db.Ping(); err != nil {
		t.Fatalf("连接数据库失败: %v", err)
	}

	if err := pbMySqlDB.OpenDB(db, mysqlConfig.DBName); err != nil {
		t.Fatalf("切换数据库失败: %v", err)
	}

	// 先确保表存在
	if _, err := db.Exec(pbMySqlDB.GetCreateTableSQL(testTable)); err != nil {
		t.Fatalf("预处理表结构失败: %v", err)
	}

	if err := pbMySqlDB.UpdateTableField(testTable); err != nil {
		t.Fatalf("执行ALTER TABLE失败: %v", err)
	}
	t.Log("ALTER TABLE成功")
}

// TestLoadSave 测试单条数据存/取
func TestLoadSave(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
	pbSave := &dbprotooption.GolangTest{
		Id:      1,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &dbprotooption.Player{
			PlayerId: 111,
			// 修复：特殊字符用双反斜杠转义
			Name: "foo\\\\0bar,foo\\\\nbar,foo\\\\rbar,foo\\\\Zbar,foo\\\\\"bar,foo\\\\\\\\bar,foo\\\\'bar",
		},
	}
	pbSave1 := &dbprotooption.GolangTest{
		Id:      2,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &dbprotooption.Player{
			PlayerId: 111,
			// 修复：特殊字符用双反斜杠转义
			Name: "foo\\\\0bar,foo\\\\nbar,foo\\\\rbar,foo\\\\Zbar,foo\\\\\"bar,foo\\\\\\\\bar,foo\\\\'bar",
		},
	}
	pbMySqlDB.RegisterTable(pbSave)

	mysqlConfig := GetMysqlConfig()
	if mysqlConfig == nil {
		t.Fatal("获取配置失败")
	}
	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		t.Fatalf("创建连接器失败: %v", err)
	}
	db := sql.OpenDB(conn)
	defer func(db *sql.DB) {
		if err := db.Close(); err != nil {
			t.Logf("关闭数据库失败: %v", err)
		}
	}(db)

	if err := db.Ping(); err != nil {
		t.Fatalf("连接数据库失败: %v", err)
	}

	if err := pbMySqlDB.OpenDB(db, mysqlConfig.DBName); err != nil {
		t.Fatalf("切换数据库失败: %v", err)
	}

	// 清理旧数据
	if _, err := db.Exec("DELETE FROM " + GetTableName(pbSave) + " WHERE id IN (1,2)"); err != nil {
		t.Logf("清理旧数据失败: %v（忽略，可能是首次执行）", err)
	}

	// 保存数据
	if err := pbMySqlDB.Save(pbSave); err != nil {
		t.Fatalf("保存pbSave失败: %v", err)
	}
	if err := pbMySqlDB.Save(pbSave1); err != nil {
		t.Fatalf("保存pbSave1失败: %v", err)
	}

	// 验证数据
	pbLoad := &dbprotooption.GolangTest{}
	if err := pbMySqlDB.FindOneByKV(pbLoad, "id", "1"); err != nil {
		t.Fatalf("读取id=1的数据失败: %v", err)
	}
	if !proto.Equal(pbSave, pbLoad) {
		t.Error("保存与读取的数据不一致（id=1）")
		t.Logf("预期: %s", pbSave.String())
		t.Logf("实际: %s", pbLoad.String())
	}

	pbLoad1 := &dbprotooption.GolangTest{}
	if err := pbMySqlDB.FindOneByKV(pbLoad1, "id", "2"); err != nil {
		t.Fatalf("读取id=2的数据失败: %v", err)
	}
	if !proto.Equal(pbSave1, pbLoad1) {
		t.Error("保存与读取的数据不一致（id=2）")
	}
}

// TestFindInsert 测试INSERT ON DUPLICATE KEY UPDATE
func TestFindInsert(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
	pbSave := &dbprotooption.GolangTest{
		Id:      1,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &dbprotooption.Player{
			PlayerId: 111,
			// 修复：特殊字符用双反斜杠转义
			Name: "foo\\\\0bar,foo\\\\nbar,foo\\\\rbar,foo\\\\Zbar,foo\\\\\"bar,foo\\\\\\\\bar,foo\\\\'bar",
		},
	}
	pbSave1 := &dbprotooption.GolangTest{
		Id:      2,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &dbprotooption.Player{
			PlayerId: 111,
			// 修复：特殊字符用双反斜杠转义
			Name: "foo\\\\0bar,foo\\\\nbar,foo\\\\rbar,foo\\\\Zbar,foo\\\\\"bar,foo\\\\\\\\bar,foo\\\\'bar",
		},
	}
	pbMySqlDB.RegisterTable(pbSave)

	mysqlConfig := GetMysqlConfig()
	if mysqlConfig == nil {
		t.Fatal("获取配置失败")
	}
	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		t.Fatalf("创建连接器失败: %v", err)
	}
	db := sql.OpenDB(conn)
	defer func(db *sql.DB) {
		if err := db.Close(); err != nil {
			t.Logf("关闭数据库失败: %v", err)
		}
	}(db)

	if err := db.Ping(); err != nil {
		t.Fatalf("连接数据库失败: %v", err)
	}

	if err := pbMySqlDB.OpenDB(db, mysqlConfig.DBName); err != nil {
		t.Fatalf("切换数据库失败: %v", err)
	}

	// 清理旧数据
	if _, err := db.Exec("DELETE FROM " + GetTableName(pbSave) + " WHERE id IN (1,2)"); err != nil {
		t.Logf("清理旧数据失败: %v", err)
	}

	// 执行插入更新
	if err := pbMySqlDB.InsertOnDupUpdate(pbSave); err != nil {
		t.Fatalf("执行InsertOnDupUpdate(pbSave)失败: %v", err)
	}
	if err := pbMySqlDB.InsertOnDupUpdate(pbSave1); err != nil {
		t.Fatalf("执行InsertOnDupUpdate(pbSave1)失败: %v", err)
	}

	// 验证数据
	pbLoad := &dbprotooption.GolangTest{}
	if err := pbMySqlDB.FindOneByKV(pbLoad, "id", "1"); err != nil {
		t.Fatalf("读取id=1失败: %v", err)
	}
	if !proto.Equal(pbSave, pbLoad) {
		t.Error("InsertOnDupUpdate后数据不一致（id=1）")
	}
}

// TestLoadByWhereCase 测试按条件查询
func TestLoadByWhereCase(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
	pbSave := &dbprotooption.GolangTest{
		Id:      1,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &dbprotooption.Player{
			PlayerId: 111,
			// 修复：特殊字符用双反斜杠转义
			Name: "foo\\\\0bar,foo\\\\nbar,foo\\\\rbar,foo\\\\Zbar,foo\\\\\"bar,foo\\\\\\\\bar,foo\\\\'bar",
		},
	}
	pbMySqlDB.RegisterTable(pbSave)

	mysqlConfig := GetMysqlConfig()
	if mysqlConfig == nil {
		t.Fatal("获取配置失败")
	}
	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		t.Fatalf("创建连接器失败: %v", err)
	}
	db := sql.OpenDB(conn)
	defer func(db *sql.DB) {
		if err := db.Close(); err != nil {
			t.Logf("关闭数据库失败: %v", err)
		}
	}(db)

	if err := db.Ping(); err != nil {
		t.Fatalf("连接数据库失败: %v", err)
	}

	if err := pbMySqlDB.OpenDB(db, mysqlConfig.DBName); err != nil {
		t.Fatalf("切换数据库失败: %v", err)
	}

	// 清理旧数据
	if _, err := db.Exec("DELETE FROM " + GetTableName(pbSave) + " WHERE id=1"); err != nil {
		t.Logf("清理旧数据失败: %v", err)
	}

	// 保存数据
	if err := pbMySqlDB.Save(pbSave); err != nil {
		t.Fatalf("保存数据失败: %v", err)
	}

	// 按条件查询（WHERE子句无需加"where"前缀）
	pbLoad := &dbprotooption.GolangTest{}
	if err := pbMySqlDB.FindOneByWhereClause(pbLoad, " WHERE id=1"); err != nil {
		t.Fatalf("执行FindOneByWhereClause失败: %v", err)
	}
	if !proto.Equal(pbSave, pbLoad) {
		t.Error("按条件查询后数据不一致")
		t.Logf("预期: %s", pbSave.String())
		t.Logf("实际: %s", pbLoad.String())
	}
}

// TestSpecialCharacterEscape 测试特殊字符存/取一致性
func TestSpecialCharacterEscape(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
	testTable := &dbprotooption.GolangTest{}
	pbMySqlDB.RegisterTable(testTable)

	mysqlConfig := GetMysqlConfig()
	if mysqlConfig == nil {
		t.Fatal("获取MySQL配置失败，请检查db.json")
	}
	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		t.Fatalf("创建MySQL连接器失败: %v", err)
	}
	db := sql.OpenDB(conn)
	defer func(db *sql.DB) {
		if err := db.Close(); err != nil {
			t.Logf("关闭数据库失败: %v", err)
		}
	}(db)

	if err := db.Ping(); err != nil {
		t.Fatalf("数据库连接失败: %v", err)
	}

	if err := pbMySqlDB.OpenDB(db, mysqlConfig.DBName); err != nil {
		t.Fatalf("切换数据库失败: %v", err)
	}

	// 测试用特殊字符集（修复：所有反斜杠用双反斜杠转义）
	specialChars := []struct {
		name  string
		value string
	}{
		{"NULL字符（\\0）", "a\\\\0b"},
		{"换行符（\\n）", "a\\\\nb"},
		{"回车符（\\r）", "a\\\\r b"},
		{"双引号（\\\"）", `a\\\\\"b`},
		{"单引号（\\'）", `a\\\\'b`},
		{"反斜杠（\\\\）", `a\\\\\\\\b`},
		{"制表符（\\t）", "a\\\\tb"},
		{"逗号（,）", "a,b"},
		{"美元符（$）", "a$b"},
		{"百分号（%）", "a%b"},
	}

	testID := uint32(1000)
	for _, sc := range specialChars {
		testID++
		// 构造测试数据
		pbSave := &dbprotooption.GolangTest{
			Id:      testID,
			GroupId: 999,
			Ip:      "192.168.1.100",
			Port:    3306,
			Player: &dbprotooption.Player{
				PlayerId: uint64(testID),
				Name:     fmt.Sprintf("Test_%s: %s", sc.name, sc.value),
			},
		}

		// 清理旧数据
		if _, err := db.Exec("DELETE FROM "+GetTableName(testTable)+" WHERE id=?", testID); err != nil {
			t.Logf("清理[%s]旧数据失败: %v", sc.name, err)
		}

		// 保存数据
		if err := pbMySqlDB.Save(pbSave); err != nil {
			t.Errorf("保存[%s]数据失败: %v, 原始值: %q", sc.name, err, sc.value)
			continue
		}

		// 读取数据
		pbLoad := &dbprotooption.GolangTest{}
		if err := pbMySqlDB.FindOneByKV(pbLoad, "id", strconv.FormatUint(uint64(testID), 10)); err != nil {
			t.Errorf("读取[%s]数据失败: %v", sc.name, err)
			continue
		}

		// 验证一致性
		if !proto.Equal(pbSave, pbLoad) {
			t.Errorf("[%s]数据不一致", sc.name)
			t.Logf("预期Name: %q", pbSave.Player.Name)
			t.Logf("实际Name: %q", pbLoad.Player.Name)
		} else {
			t.Logf("[%s]测试通过，原始值: %q", sc.name, sc.value)
		}
	}
}

// TestStringWithSpaces 测试空格处理
func TestStringWithSpaces(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
	testTable := &dbprotooption.GolangTest{}
	pbMySqlDB.RegisterTable(testTable)

	mysqlConfig := GetMysqlConfig()
	if mysqlConfig == nil {
		t.Fatal("获取配置失败")
	}
	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		t.Fatalf("创建连接器失败: %v", err)
	}
	db := sql.OpenDB(conn)
	defer func(db *sql.DB) {
		if err := db.Close(); err != nil {
			t.Logf("关闭数据库失败: %v", err)
		}
	}(db)

	if err := db.Ping(); err != nil {
		t.Fatalf("连接数据库失败: %v", err)
	}

	if err := pbMySqlDB.OpenDB(db, mysqlConfig.DBName); err != nil {
		t.Fatalf("切换数据库失败: %v", err)
	}

	// 测试用例
	testCases := []struct {
		id   int32
		name string
		desc string
	}{
		{2001, "Single space between words", "单个空格"},
		{2002, "  Double  spaces  between  words  ", "前后双空格+中间双空格"},
		{2003, " Leading space", "前导空格"},
		{2004, "Trailing space ", "尾随空格"},
		// 修复：制表符、换行符用双反斜杠转义
		{2005, "Mixed\\\\tspaces\\\\nand\\\\vother\\\\fwhitespace", "混合空白符"},
	}

	for _, tc := range testCases {
		// 清理旧数据
		if _, err := db.Exec("DELETE FROM "+GetTableName(testTable)+" WHERE id=?", tc.id); err != nil {
			t.Logf("清理[%s]旧数据失败: %v", tc.desc, err)
		}

		// 保存数据
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
		if err := pbMySqlDB.Save(pbSave); err != nil {
			t.Errorf("保存[%s]数据失败: %v", tc.desc, err)
			continue
		}

		// 读取数据
		pbLoad := &dbprotooption.GolangTest{}
		if err := pbMySqlDB.FindOneByKV(pbLoad, "id", strconv.FormatInt(int64(tc.id), 10)); err != nil {
			t.Errorf("读取[%s]数据失败: %v", tc.desc, err)
			continue
		}

		// 验证空格一致性
		if pbLoad.Player.Name != tc.name {
			t.Errorf("[%s]空格处理不一致", tc.desc)
			t.Logf("预期: %q (长度: %d)", tc.name, len(tc.name))
			t.Logf("实际: %q (长度: %d)", pbLoad.Player.Name, len(pbLoad.Player.Name))
		} else {
			t.Logf("[%s]测试通过", tc.desc)
		}
	}
}

// TestLoadSaveListWhereCase 测试批量查询
func TestLoadSaveListWhereCase(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
	testTable := &dbprotooption.GolangTest{}
	pbMySqlDB.RegisterTable(testTable)

	mysqlConfig := GetMysqlConfig()
	if mysqlConfig == nil {
		t.Fatal("获取配置失败")
	}
	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		t.Fatalf("创建连接器失败: %v", err)
	}
	db := sql.OpenDB(conn)
	defer func(db *sql.DB) {
		if err := db.Close(); err != nil {
			t.Logf("关闭数据库失败: %v", err)
		}
	}(db)

	if err := db.Ping(); err != nil {
		t.Fatalf("连接数据库失败: %v", err)
	}

	if err := pbMySqlDB.OpenDB(db, mysqlConfig.DBName); err != nil {
		t.Fatalf("切换数据库失败: %v", err)
	}

	// 构造预期数据
	expectedList := &dbprotooption.GolangTestList{
		TestList: []*dbprotooption.GolangTest{
			{
				Id:      101,
				GroupId: 1,
				Ip:      "127.0.0.1",
				Port:    3306,
				Player: &dbprotooption.Player{
					PlayerId: 1001,
					Name:     "BatchTest_1",
				},
			},
			{
				Id:      102,
				GroupId: 1,
				Ip:      "127.0.0.1",
				Port:    3306,
				Player: &dbprotooption.Player{
					PlayerId: 1002,
					Name:     "BatchTest_2",
				},
			},
		},
	}

	// 清理旧数据
	if _, err := db.Exec("DELETE FROM " + GetTableName(testTable) + " WHERE group_id=1"); err != nil {
		t.Logf("清理批量测试旧数据失败: %v", err)
	}

	// 批量保存
	for _, item := range expectedList.TestList {
		if err := pbMySqlDB.Save(item); err != nil {
			t.Fatalf("批量保存数据失败（id=%d）: %v", item.Id, err)
		}
	}

	// 批量查询
	actualList := &dbprotooption.GolangTestList{}
	if err := pbMySqlDB.FindAllByWhereClause(actualList, " WHERE group_id=1"); err != nil {
		t.Fatalf("批量查询失败: %v", err)
	}

	// 验证数量
	if len(actualList.TestList) != len(expectedList.TestList) {
		t.Fatalf("批量查询结果数量不一致，预期%d条，实际%d条", len(expectedList.TestList), len(actualList.TestList))
	}

	// 按ID排序（避免顺序问题）
	sort.Slice(expectedList.TestList, func(i, j int) bool {
		return expectedList.TestList[i].Id < expectedList.TestList[j].Id
	})
	sort.Slice(actualList.TestList, func(i, j int) bool {
		return actualList.TestList[i].Id < actualList.TestList[j].Id
	})

	// 逐条验证
	for i := range expectedList.TestList {
		if !proto.Equal(expectedList.TestList[i], actualList.TestList[i]) {
			t.Errorf("批量查询第%d条数据不一致", i+1)
			t.Logf("预期: %s", expectedList.TestList[i].String())
			t.Logf("实际: %s", actualList.TestList[i].String())
		}
	}
	t.Log("批量查询测试通过")
}
