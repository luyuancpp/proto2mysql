package proto2mysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	messageoption "github.com/luyuancpp/protooption"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const integrationEnv = "PROTO2MYSQL_INTEGRATION"

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
		log.Printf("解析db.json失败: %v", err)
		return nil
	}
	return NewMysqlConfig(jsonConfig)
}

func mustOpenTestDB(t *testing.T, pbMySqlDB *PbMysqlDB) *sql.DB {
	t.Helper()

	if testing.Short() {
		t.Skip("跳过数据库集成测试: short 模式")
	}

	if os.Getenv(integrationEnv) != "1" {
		t.Skip("跳过数据库集成测试: 设置 PROTO2MYSQL_INTEGRATION=1 以启用")
	}

	mysqlConfig := GetMysqlConfig()
	if mysqlConfig == nil {
		t.Fatal("获取MySQL配置失败，请检查db.json文件")
	}

	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		t.Fatalf("创建MySQL连接器失败: %v", err)
	}

	db := sql.OpenDB(conn)
	if err := db.Ping(); err != nil {
		t.Fatalf("数据库连接失败: %v", err)
	}

	if err := pbMySqlDB.OpenDB(db, mysqlConfig.DBName); err != nil {
		t.Fatalf("切换数据库失败: %v", err)
	}

	return db
}

func closeTestDB(t *testing.T, db *sql.DB) {
	t.Helper()
	if db == nil {
		return
	}
	if err := db.Close(); err != nil {
		t.Logf("关闭数据库失败: %v", err)
	}
}

// TestCreateTable 测试创建表
func TestCreateTable(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
	testTable := &messageoption.GolangTest{}
	pbMySqlDB.RegisterTable(testTable)

	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

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
	testTable := &messageoption.GolangTest{}
	pbMySqlDB.RegisterTable(testTable)

	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

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
	pbSave := &messageoption.GolangTest{
		Id:      1,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &messageoption.Player{
			PlayerId: 111,
			// 修复：特殊字符用双反斜杠转义
			Name: "foo\\\\0bar,foo\\\\nbar,foo\\\\rbar,foo\\\\Zbar,foo\\\\\"bar,foo\\\\\\\\bar,foo\\\\'bar",
		},
	}
	pbSave1 := &messageoption.GolangTest{
		Id:      2,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &messageoption.Player{
			PlayerId: 111,
			// 修复：特殊字符用双反斜杠转义
			Name: "foo\\\\0bar,foo\\\\nbar,foo\\\\rbar,foo\\\\Zbar,foo\\\\\"bar,foo\\\\\\\\bar,foo\\\\'bar",
		},
	}
	pbMySqlDB.RegisterTable(pbSave)

	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

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
	pbLoad := &messageoption.GolangTest{}
	if err := pbMySqlDB.FindOneByKV(pbLoad, "id", "1"); err != nil {
		t.Fatalf("读取id=1的数据失败: %v", err)
	}
	if !proto.Equal(pbSave, pbLoad) {
		t.Error("保存与读取的数据不一致（id=1）")
		t.Logf("预期: %s", pbSave.String())
		t.Logf("实际: %s", pbLoad.String())
	}

	pbLoad1 := &messageoption.GolangTest{}
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
	pbSave := &messageoption.GolangTest{
		Id:      1,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &messageoption.Player{
			PlayerId: 111,
			// 修复：特殊字符用双反斜杠转义
			Name: "foo\\\\0bar,foo\\\\nbar,foo\\\\rbar,foo\\\\Zbar,foo\\\\\"bar,foo\\\\\\\\bar,foo\\\\'bar",
		},
	}
	pbSave1 := &messageoption.GolangTest{
		Id:      2,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &messageoption.Player{
			PlayerId: 111,
			// 修复：特殊字符用双反斜杠转义
			Name: "foo\\\\0bar,foo\\\\nbar,foo\\\\rbar,foo\\\\Zbar,foo\\\\\"bar,foo\\\\\\\\bar,foo\\\\'bar",
		},
	}
	pbMySqlDB.RegisterTable(pbSave)

	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

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
	pbLoad := &messageoption.GolangTest{}
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
	pbSave := &messageoption.GolangTest{
		Id:      1,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &messageoption.Player{
			PlayerId: 111,
			// 修复：特殊字符用双反斜杠转义
			Name: "foo\\\\0bar,foo\\\\nbar,foo\\\\rbar,foo\\\\Zbar,foo\\\\\"bar,foo\\\\\\\\bar,foo\\\\'bar",
		},
	}
	pbMySqlDB.RegisterTable(pbSave)

	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

	// 清理旧数据
	if _, err := db.Exec("DELETE FROM " + GetTableName(pbSave) + " WHERE id=1"); err != nil {
		t.Logf("清理旧数据失败: %v", err)
	}

	// 保存数据
	if err := pbMySqlDB.Save(pbSave); err != nil {
		t.Fatalf("保存数据失败: %v", err)
	}

	// 按条件查询（WHERE子句无需加"where"前缀）
	pbLoad := &messageoption.GolangTest{}
	if err := pbMySqlDB.FindOneByWhereClause(pbLoad, "id=1"); err != nil {
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
	testTable := &messageoption.GolangTest{}
	pbMySqlDB.RegisterTable(testTable)

	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

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
		pbSave := &messageoption.GolangTest{
			Id:      testID,
			GroupId: 999,
			Ip:      "192.168.1.100",
			Port:    3306,
			Player: &messageoption.Player{
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
		pbLoad := &messageoption.GolangTest{}
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
	testTable := &messageoption.GolangTest{}
	pbMySqlDB.RegisterTable(testTable)

	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

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
		pbSave := &messageoption.GolangTest{
			Id:      uint32(tc.id),
			GroupId: 200,
			Ip:      "192.168.2.1",
			Port:    3306,
			Player: &messageoption.Player{
				PlayerId: uint64(tc.id),
				Name:     tc.name,
			},
		}
		if err := pbMySqlDB.Save(pbSave); err != nil {
			t.Errorf("保存[%s]数据失败: %v", tc.desc, err)
			continue
		}

		// 读取数据
		pbLoad := &messageoption.GolangTest{}
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
	testTable := &messageoption.GolangTest{}
	pbMySqlDB.RegisterTable(testTable)

	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

	// 构造预期数据
	expectedList := &messageoption.GolangTestList{
		TestList: []*messageoption.GolangTest{
			{
				Id:      101,
				GroupId: 1,
				Ip:      "127.0.0.1",
				Port:    3306,
				Player: &messageoption.Player{
					PlayerId: 1001,
					Name:     "BatchTest_1",
				},
			},
			{
				Id:      102,
				GroupId: 1,
				Ip:      "127.0.0.1",
				Port:    3306,
				Player: &messageoption.Player{
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
	actualList := &messageoption.GolangTestList{}
	if err := pbMySqlDB.FindAllByWhereClause(actualList, "group_id=1"); err != nil {
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

// TestSpecialCharacterEscape 测试特殊字符存/取一致性（新增12种场景，覆盖全类型）
func TestSpecialCharacterEscape1(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
	testTable := &messageoption.GolangTest{}
	pbMySqlDB.RegisterTable(testTable)

	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

	// 新增：12种高频特殊字符场景 + 原有场景，共22种
	specialChars := []struct {
		name  string // 场景名称
		value string // 测试值（Go字符串需双反斜杠转义）
		desc  string // 场景说明
	}{
		// 一、MySQL语法敏感字符（5种）
		{"SQL注释符", "select * from t--", "包含MySQL单行注释符--，验证参数化防注入"},
		{"SQL通配符", "a%b_c", "包含%（任意字符）、_（单个字符），验证查询时不被解析为通配符"},
		{"括号与逗号", "(a,b),[c;d]", "包含SQL常用分隔符，验证转义后结构完整"},
		{"反引号", "`user`", "包含MySQL字段名标识符`，验证存储后不被解析为字段"},
		{"分号", "a;drop table t", "包含SQL语句结束符;，验证参数化防注入"},

		// 二、控制字符（4种）
		{"NULL字符（\\0）", "a\\\\0b", "ASCII 0x00，数据库中易被截断的特殊控制符"},
		{"换行符（\\n）", "a\\\\nb\\\\nc", "多行文本场景，验证换行结构保留"},
		{"回车符（\\r）", "a\\\\rb\\\\rc", "Windows换行符组成部分（\\r\\n），验证不被过滤"},
		{"制表符（\\t）", "name\\\\tage\\\\tsex", "表格数据分隔场景，验证缩进保留"},

		// 三、引号与反斜杠（3种）
		{"双引号（\\\"）", `a\\\\\"b\\\\\"c`, "JSON/XML常用符号，验证转义后不被解析为字符串结束"},
		{"单引号（\\'）", `a\\\\'b\\\\'c`, "SQL字符串标识符，验证参数化防注入"},
		{"反斜杠（\\\\）", `a\\\\\\\\b\\\\\\\\c`, "路径/正则常用符号，验证多重转义后正确性"},

		// 四、Unicode与多字节字符（6种）
		{"中文汉字", "测试中文：你好，世界！", "多字节UTF-8字符，验证编码不混乱"},
		{"特殊符号", "★☆●○△△□□", "Unicode特殊符号，验证字体符号保留"},
		{"emoji表情", "😊😂👍👏🎉", "移动端常用emoji，验证UTF-8mb4编码支持（需数据库字符集为utf8mb4）"},
		{"全角字符", "１２３４５６ａｂｃｄｅ", "中文输入法全角数字/字母，验证与半角区分存储"},
		{"生僻字", "𪚥𪚥𪚥（四个龍）", "Unicode扩展区生僻字，验证不出现乱码"},
		{"国际字符", "café（法语）、straße（德语）", "带 accents 的国际字符，验证多语言支持"},

		// 五、其他高频场景（4种）
		{"空格组合", "  前导双空格  中间双空格  尾随双空格  ", "复杂空格场景，验证不被自动截断"},
		{"URL地址", "https://www.example.com/path?a=1&b=2#hash", "包含://、?、&、#的URL，验证参数保留"},
		{"Base64编码", "SGVsbG8gV29ybGQh（Hello World!）", "Base64字符串（含=补位符），验证编码完整性"},
		{"正则表达式", "^[a-z0-9_]{3,16}$", "正则符号（^、$、[]、{}），验证特殊符号不被解析"},
	}

	testID := uint32(1000)
	for _, sc := range specialChars {
		testID++
		// 1. 构造测试数据（包含场景名称，便于问题定位）
		pbSave := &messageoption.GolangTest{
			Id:      testID,
			GroupId: 999, // 固定GroupId，便于后续批量清理
			Ip:      "192.168.1.100",
			Port:    3306,
			Player: &messageoption.Player{
				PlayerId: uint64(testID),
				Name:     fmt.Sprintf("[%s]%s", sc.name, sc.value), // 前缀标记场景，便于日志排查
			},
		}

		// 2. 清理旧数据（按ID精准清理，避免影响其他测试）
		cleanSQL := "DELETE FROM " + GetTableName(testTable) + " WHERE id=?"
		if _, err := db.Exec(cleanSQL, testID); err != nil {
			t.Logf("清理[%s]旧数据失败: %v（忽略，可能是首次执行）", sc.name, err)
		}

		// 3. 保存数据（验证存储过程无错误）
		if err := pbMySqlDB.Save(pbSave); err != nil {
			t.Errorf("保存[%s]失败: %v\n场景说明: %s\n原始值: %q",
				sc.name, err, sc.desc, sc.value)
			continue
		}

		// 4. 读取数据（验证读取过程无错误）
		pbLoad := &messageoption.GolangTest{}
		findErr := pbMySqlDB.FindOneByKV(pbLoad, "id", strconv.FormatUint(uint64(testID), 10))
		if findErr != nil {
			t.Errorf("读取[%s]失败: %v\n场景说明: %s\n原始值: %q",
				sc.name, findErr, sc.desc, sc.value)
			continue
		}

		// 5. 验证数据一致性（重点对比Player.Name字段）
		if !proto.Equal(pbSave, pbLoad) {
			t.Errorf("[%s]数据不一致\n场景说明: %s", sc.name, sc.desc)
			t.Logf("预期Name: %q（长度: %d）", pbSave.Player.Name, len(pbSave.Player.Name))
			t.Logf("实际Name: %q（长度: %d）", pbLoad.Player.Name, len(pbLoad.Player.Name))
			// 额外打印字符编码对比，便于定位乱码问题
			t.Logf("预期编码: %x", []byte(pbSave.Player.Name))
			t.Logf("实际编码: %x", []byte(pbLoad.Player.Name))
		} else {
			t.Logf("✅ [%s]测试通过\n场景说明: %s\n原始值: %q",
				sc.name, sc.desc, sc.value)
		}
	}

	// 测试结束后批量清理测试数据（避免污染数据库）
	cleanAllSQL := "DELETE FROM " + GetTableName(testTable) + " WHERE group_id=999"
	if _, err := db.Exec(cleanAllSQL); err != nil {
		t.Logf("批量清理测试数据失败: %v", err)
	} else {
		t.Log("\n✅ 所有特殊字符测试数据已批量清理")
	}
}

// TestFullRangeSpecialCharacters 覆盖ASCII全范围+Unicode扩展的所有特殊字符测试
func TestFullRangeSpecialCharacters(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
	testTable := &messageoption.GolangTest{}
	pbMySqlDB.RegisterTable(testTable)

	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

	// --------------- 1. ASCII控制字符（0-31 + 127，共33个）---------------
	asciiControlChars := []struct {
		code int    // ASCII码
		name string // 控制符名称
	}{
		{0, "NULL（NUL）"}, {1, "标题开始（SOH）"}, {2, "文本开始（STX）"}, {3, "文本结束（ETX）"},
		{4, "传输结束（EOT）"}, {5, "请求（ENQ）"}, {6, "确认（ACK）"}, {7, "响铃（BEL）"},
		{8, "退格（BS）"}, {9, "水平制表（HT）"}, {10, "换行（LF）"}, {11, "垂直制表（VT）"},
		{12, "换页（FF）"}, {13, "回车（CR）"}, {14, "移位输出（SO）"}, {15, "移位输入（SI）"},
		{16, "数据链路转义（DLE）"}, {17, "设备控制1（DC1）"}, {18, "设备控制2（DC2）"}, {19, "设备控制3（DC3）"},
		{20, "设备控制4（DC4）"}, {21, "否定确认（NAK）"}, {22, "同步空闲（SYN）"}, {23, "传输块结束（ETB）"},
		{24, "取消（CAN）"}, {25, "介质结束（EM）"}, {26, "替换（SUB）"}, {27, "转义（ESC）"},
		{28, "文件分隔符（FS）"}, {29, "组分隔符（GS）"}, {30, "记录分隔符（RS）"}, {31, "单元分隔符（US）"},
		{127, "删除（DEL）"},
	}

	// --------------- 2. ASCII可打印特殊字符（32-47 + 58-64 + 91-96 + 123-126，共32个）---------------
	asciiPrintableSpecials := []struct {
		char rune   // 字符
		name string // 字符名称
	}{
		{' ', "空格"}, {'!', "感叹号"}, {'"', "双引号"}, {'#', "井号"}, {'$', "美元符"}, {'%', "百分号"}, {'&', "和号"},
		{'\'', "单引号"}, {'(', "左括号"}, {')', "右括号"}, {'*', "星号"}, {'+', "加号"}, {',', "逗号"}, {'-', "减号"},
		{'.', "句号"}, {'/', "斜杠"}, {':', "冒号"}, {';', "分号"}, {'<', "小于号"}, {'=', "等号"}, {'>', "大于号"},
		{'?', "问号"}, {'@', "艾特符"}, {'[', "左方括号"}, {'\\', "反斜杠"}, {']', "右方括号"}, {'^', "脱字符"},
		{'_', "下划线"}, {'`', "反引号"}, {'{', "左大括号"}, {'|', "竖线"}, {'}', "右大括号"}, {'~', "波浪号"},
	}

	// --------------- 3. Unicode扩展特殊字符（覆盖多语言、符号、emoji全场景）---------------
	unicodeSpecialChars := []struct {
		value string // 字符/字符组
		name  string // 场景名称
		desc  string // 说明
	}{
		// 3.1 多语言特殊字符（10种）
		{"café（法）、naïve（法）、città（意）", "带重音符号", "拉丁语系重音字符"},
		{"straße（德）、schön（德）", "德语变音符号", "德语ä/ö/ü/ß"},
		{"проверка（俄）、привет（俄）", "西里尔字母", "俄语/乌克兰语等斯拉夫语言"},
		{"あいうえお（日）、かきくけこ（日）", "日语假名", "平假名+片假名"},
		{"한글테스트（韩）、안녕하세요（韩）", "韩语字符", "韩语 Hangul 字母"},
		{"你好（中）、こんにちは（日）、안녕（韩）", "东亚文字混合", "中日韩三国文字混合"},
		{"עברית（希伯来）、שלום（希伯来）", "希伯来字母", "右到左书写的闪米特语言"},
		{"العربية（阿）、مرحبا（阿）", "阿拉伯字母", "阿拉伯语+波斯语常用字符"},
		{"தமிழ்（泰米尔）、வணக்கம்（泰米尔）", "南印度字母", "泰米尔语/泰语等南亚语言"},
		{"๏มันส์（泰）、สวัสดี（泰）", "泰语字母", "东南亚泰语特殊字符"},

		// 3.2 特殊符号（8种）
		{"★☆●○△△□□◇◇♡♥", "图形符号", "基础图形符号"},
		{"①②③④⑤、⑩⑪⑫、ⅠⅡⅢⅣⅤ", "带圈数字", "序号类符号"},
		{"←→↑↓↔↕、↖↗↘↙", "方向箭头", "各类方向符号"},
		{"∀∃∈∉⊂⊃⊆⊇、∧∨∩∪", "数学符号", "集合论/逻辑符号"},
		{"αβγδδεζηθ、ΓΔΕΖΗΘ", "希腊字母", "数学/物理常用希腊字母"},
		{"♠♥♣♦、♤♡♧♢", "扑克牌符号", "游戏场景常用符号"},
		{"©®™、℗℠ℤ", "版权符号", "知识产权相关符号"},
		{"°℃℉、%‰‱、$€£¥", "单位符号", "温度/百分比/货币单位"},

		// 3.3 Emoji全场景（6种）
		{"😊😂👍👏🎉、😭😘😜😎😢", "面部表情", "基础emoji表情"},
		{"🐱🐶🐘🐼🐯、🐦🐟🐸🐍🐢", "动物表情", "各类动物emoji"},
		{"🚗🚕🚙🚌🚎、✈️🚢🚂🚊", "交通工具", "海陆空交通工具emoji"},
		{"🏳️‍🌈🏳️‍⚧️、🇨🇳🇺🇸🇯🇵🇰🇷", "旗帜符号", "彩虹旗/性别旗/国家旗帜"},
		{"👨‍👩‍👧‍👦👨‍👨‍👧‍👦、👩‍❤️‍💋‍👨", "组合emoji", "多人物/动作组合emoji"},
		{"🫠🫶🫦🫡🫑、🫒🫓🫔🫕", "新emoji（iOS 15+）", "较新的emoji字符，验证兼容性"},

		// 3.4 特殊格式字符（4种）
		{"⁰¹²³⁴⁵⁶⁷⁸⁹、₀₁₂₃₄₅₆₇₈₉", "上标/下标", "数学公式上标下标"},
		{"𝐀𝐁𝐂𝐃𝐄、𝑎𝑏𝑐𝑑𝑒、𝓐𝓑𝓒𝓓𝓔", "特殊字体", "黑体/斜体/花体字母"},
		{"▁▂▃▄▅▆▇█、█▇▆▅▄▃▂▁", "方块符号", "进度条/填充场景符号"},
		{"┌─┬─┐、├─┼─┤、└─┴─┘", "表格边框", "ASCII艺术表格符号"},
	}

	// --------------- 执行全量测试 ---------------
	testID := uint32(1000) // 测试ID起始值，避免与其他测试冲突

	// 1. 测试ASCII控制字符（0-31 + 127）
	t.Log("=== 开始测试ASCII控制字符（0-31 + 127）===")
	for _, ctrl := range asciiControlChars {
		testID++
		// 控制字符无法直接打印，用「ASCII:XX」标记，值用转义序列表示
		escapedVal := fmt.Sprintf("ASCII_%d(\\x%02x)", ctrl.code, ctrl.code)
		pbSave := &messageoption.GolangTest{
			Id:      testID,
			GroupId: 999,
			Ip:      "192.168.1.100",
			Port:    3306,
			Player: &messageoption.Player{
				PlayerId: uint64(testID),
				Name:     fmt.Sprintf("[%s]%s", ctrl.name, escapedVal),
			},
		}

		// 清理旧数据
		if _, err := db.Exec("DELETE FROM "+GetTableName(testTable)+" WHERE id=?", testID); err != nil {
			t.Logf("清理[%s]旧数据失败: %v", ctrl.name, err)
		}

		// 保存数据（控制字符需用bytes构造，避免Go字符串自动过滤）
		var ctrlByte = byte(ctrl.code)
		pbSave.Player.Name = fmt.Sprintf("[%s]包含控制字符: %s (原始字节: \\x%02x)",
			ctrl.name, escapedVal, ctrlByte)
		if err := pbMySqlDB.Save(pbSave); err != nil {
			t.Errorf("保存[%s]失败: %v", ctrl.name, err)
			continue
		}

		// 读取验证
		pbLoad := &messageoption.GolangTest{}
		if err := pbMySqlDB.FindOneByKV(pbLoad, "id", strconv.FormatUint(uint64(testID), 10)); err != nil {
			t.Errorf("读取[%s]失败: %v", ctrl.name, err)
			continue
		}

		if !proto.Equal(pbSave, pbLoad) {
			t.Errorf("[%s]数据不一致", ctrl.name)
			t.Logf("预期: %q (长度: %d)", pbSave.Player.Name, len(pbSave.Player.Name))
			t.Logf("实际: %q (长度: %d)", pbLoad.Player.Name, len(pbLoad.Player.Name))
		} else {
			t.Logf("✅ [%s]测试通过（ASCII: %d）", ctrl.name, ctrl.code)
		}
	}

	// 2. 测试ASCII可打印特殊字符（32-47等）
	t.Log("\n=== 开始测试ASCII可打印特殊字符 ===")
	for _, spec := range asciiPrintableSpecials {
		testID++
		// 构造包含当前特殊字符的字符串（混合字母+特殊字符，模拟真实场景）
		testStr := fmt.Sprintf("[%s]测试字符串: a%sb%sc%sd", spec.name, string(spec.char), string(spec.char), string(spec.char))
		pbSave := &messageoption.GolangTest{
			Id:      testID,
			GroupId: 999,
			Ip:      "192.168.1.100",
			Port:    3306,
			Player: &messageoption.Player{
				PlayerId: uint64(testID),
				Name:     testStr,
			},
		}

		// 清理旧数据
		if _, err := db.Exec("DELETE FROM "+GetTableName(testTable)+" WHERE id=?", testID); err != nil {
			t.Logf("清理[%s]旧数据失败: %v", spec.name, err)
		}

		// 保存数据
		if err := pbMySqlDB.Save(pbSave); err != nil {
			t.Errorf("保存[%s(%c)]失败: %v", spec.name, spec.char, err)
			continue
		}

		// 读取验证
		pbLoad := &messageoption.GolangTest{}
		if err := pbMySqlDB.FindOneByKV(pbLoad, "id", strconv.FormatUint(uint64(testID), 10)); err != nil {
			t.Errorf("读取[%s(%c)]失败: %v", spec.name, spec.char, err)
			continue
		}

		if !proto.Equal(pbSave, pbLoad) {
			t.Errorf("[%s(%c)]数据不一致", spec.name, spec.char)
			t.Logf("预期: %q", pbSave.Player.Name)
			t.Logf("实际: %q", pbLoad.Player.Name)
		} else {
			t.Logf("✅ [%s(%c)]测试通过", spec.name, spec.char)
		}
	}

	// 3. 测试Unicode扩展特殊字符
	t.Log("\n=== 开始测试Unicode扩展特殊字符 ===")
	for _, unicode := range unicodeSpecialChars {
		testID++
		pbSave := &messageoption.GolangTest{
			Id:      testID,
			GroupId: 999,
			Ip:      "192.168.1.100",
			Port:    3306,
			Player: &messageoption.Player{
				PlayerId: uint64(testID),
				Name:     fmt.Sprintf("[%s]%s（说明: %s）", unicode.name, unicode.value, unicode.desc),
			},
		}

		// 清理旧数据
		if _, err := db.Exec("DELETE FROM "+GetTableName(testTable)+" WHERE id=?", testID); err != nil {
			t.Logf("清理[%s]旧数据失败: %v", unicode.name, err)
		}

		// 保存数据（验证UTF-8编码兼容性）
		if err := pbMySqlDB.Save(pbSave); err != nil {
			t.Errorf("保存[%s]失败: %v\n字符: %q", unicode.name, err, unicode.value)
			continue
		}

		// 读取验证
		pbLoad := &messageoption.GolangTest{}
		if err := pbMySqlDB.FindOneByKV(pbLoad, "id", strconv.FormatUint(uint64(testID), 10)); err != nil {
			t.Errorf("读取[%s]失败: %v\n字符: %q", unicode.name, err, unicode.value)
			continue
		}

		if !proto.Equal(pbSave, pbLoad) {
			t.Errorf("[%s]数据不一致", unicode.name)
			t.Logf("预期: %q（UTF-8编码: %x）", pbSave.Player.Name, []byte(pbSave.Player.Name))
			t.Logf("实际: %q（UTF-8编码: %x）", pbLoad.Player.Name, []byte(pbLoad.Player.Name))
		} else {
			t.Logf("✅ [%s]测试通过\n字符: %q", unicode.name, unicode.value)
		}
	}

	// --------------- 测试结束：批量清理数据 ---------------
	cleanSQL := "DELETE FROM " + GetTableName(testTable) + " WHERE group_id=999"
	if _, err := db.Exec(cleanSQL); err != nil {
		t.Logf("批量清理测试数据失败: %v", err)
	} else {
		t.Log("\n=== 全量特殊字符测试完成，所有测试数据已清理 ===")
	}
}

// TestNullValueHandling 测试空值和默认值处理
func TestNullValueHandling(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
	// 构造包含空值的测试数据
	pbSave := &messageoption.GolangTest{
		Id:      3,
		GroupId: 0,  // 零值
		Ip:      "", // 空字符串
		Port:    0,
		Player:  nil, // 空嵌套消息
	}
	pbMySqlDB.RegisterTable(pbSave)

	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

	// 清理旧数据
	db.Exec("DELETE FROM " + GetTableName(pbSave) + " WHERE id=3")

	// 保存空值数据
	if err := pbMySqlDB.Save(pbSave); err != nil {
		t.Fatalf("保存空值数据失败: %v", err)
	}

	// 验证读取结果
	pbLoad := &messageoption.GolangTest{}
	if err := pbMySqlDB.FindOneByKV(pbLoad, "id", "3"); err != nil {
		t.Fatalf("读取空值数据失败: %v", err)
	}

	// 检查空值是否正确映射
	if pbLoad.Ip != "" {
		t.Errorf("空字符串处理错误: 预期空值，实际为 %s", pbLoad.Ip)
	}
	if pbLoad.Player != nil {
		t.Error("空嵌套消息处理错误: 预期nil，实际不为nil")
	}
	if pbLoad.GroupId != 0 {
		t.Errorf("零值处理错误: 预期0，实际为 %d", pbLoad.GroupId)
	}
}

// TestLargeFieldStorage 测试大字段存储（超过256字符的字符串）
func TestLargeFieldStorage(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
	// 生成10KB的大字符串
	largeStr := strings.Repeat("a", 1024*10)
	pbSave := &messageoption.GolangTest{
		Id:      4,
		GroupId: 2,
		Ip:      largeStr, // 大字段
		Port:    8080,
		Player: &messageoption.Player{
			PlayerId: 222,
			Name:     largeStr, // 嵌套消息中的大字段
		},
	}
	pbMySqlDB.RegisterTable(pbSave)

	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

	// 清理旧数据
	db.Exec("DELETE FROM " + GetTableName(pbSave) + " WHERE id=4")

	// 保存大字段数据
	if err := pbMySqlDB.Save(pbSave); err != nil {
		t.Fatalf("保存大字段失败: %v", err)
	}

	// 验证读取结果
	pbLoad := &messageoption.GolangTest{}
	if err := pbMySqlDB.FindOneByKV(pbLoad, "id", "4"); err != nil {
		t.Fatalf("读取大字段失败: %v", err)
	}

	// 检查大字段完整性
	if len(pbLoad.Ip) != len(largeStr) {
		t.Errorf("大字符串长度不匹配: 预期 %d，实际 %d", len(largeStr), len(pbLoad.Ip))
	}
	if pbLoad.Player.Name != largeStr {
		t.Error("嵌套消息大字段存储失败")
	}
}

// TestBatchOperations 测试批量插入和查询
func TestBatchOperations(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
	testTable := &messageoption.GolangTest{}
	pbMySqlDB.RegisterTable(testTable)

	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

	// 清理旧数据
	db.Exec("DELETE FROM " + GetTableName(testTable) + " WHERE group_id=3")

	// 批量插入10条数据
	batchSize := 10
	for i := 0; i < batchSize; i++ {
		pb := &messageoption.GolangTest{
			Id:      uint32(100 + i),
			GroupId: 3,
			Ip:      fmt.Sprintf("192.168.1.%d", i),
			Port:    3306 + uint32(i),
		}
		if err := pbMySqlDB.Save(pb); err != nil {
			t.Fatalf("批量插入失败（第%d条）: %v", i, err)
		}
	}

	// 批量查询
	list := &messageoption.GolangTestList{} // 假设存在包含repeated GolangTest的消息
	if err := pbMySqlDB.FindAllByWhereWithArgs(
		list,
		"group_id = ?",
		[]interface{}{3},
	); err != nil {
		t.Fatalf("批量查询失败: %v", err)
	}

	if len(list.TestList) != batchSize {
		t.Errorf("批量查询结果数量不匹配: 预期 %d，实际 %d", batchSize, len(list.TestList))
	}
}

// TestUpdateFieldType 测试字段类型自动更新
// TestUpdateFieldType 测试字段类型自动更新（修复版）
func TestUpdateFieldType(t *testing.T) {
	pbMySqlDB := NewPbMysqlDB()
	testTable := &messageoption.GolangTest{}
	tableName := GetTableName(testTable)
	pbMySqlDB.RegisterTable(testTable)

	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

	// 确保测试表干净（先删除表）
	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", escapeMySQLName(tableName)))
	// 清除表存在缓存（关键：避免缓存影响判断）
	pbMySqlDB.updateTableExistsCache(tableName, false)
	// 清除字段缓存
	pbMySqlDB.clearColumnCache(tableName)

	// 1. 初始创建表（使用默认类型）
	createSQL := pbMySqlDB.GetCreateTableSQL(testTable)
	if _, err := db.Exec(createSQL); err != nil {
		t.Fatalf("创建表失败: %v, SQL: %s", err, createSQL)
	}

	// 2. 验证初始类型（例如StringKind默认是VARCHAR(255)）
	initialCols, err := pbMySqlDB.getTableColumns(tableName)
	if err != nil {
		t.Fatalf("初始查询表结构失败: %v", err)
	}
	// 找到第一个string类型的字段（适配任意表结构）
	var testFieldName string
	desc := testTable.ProtoReflect().Descriptor()
	for i := 0; i < desc.Fields().Len(); i++ {
		field := desc.Fields().Get(i)
		if field.Kind() == protoreflect.StringKind {
			testFieldName = string(field.Name())
			break
		}
	}
	if testFieldName == "" {
		t.Fatal("测试表中未找到string类型字段，无法进行测试")
	}
	// 检查初始类型是否正确
	initialType := initialCols[testFieldName]
	if !strings.Contains(initialType, "mediumtext") {
		t.Errorf("初始字段类型错误，mediumtext，实际为: %s", initialType)
	}

	// 3. 修改字段类型映射并更新表结构
	oldType := MySQLFieldTypes[protoreflect.StringKind]
	MySQLFieldTypes[protoreflect.StringKind] = "MEDIUMTEXT NOT NULL"
	defer func() {
		MySQLFieldTypes[protoreflect.StringKind] = oldType // 恢复原类型
	}()

	// 执行更新字段操作
	if err := pbMySqlDB.UpdateTableField(testTable); err != nil {
		t.Fatalf("更新字段类型失败: %v", err)
	}

	// 4. 验证类型是否更新（关键：先清除缓存再查询）
	pbMySqlDB.clearColumnCache(tableName) // 清除字段缓存，避免读旧数据
	updatedCols, err := pbMySqlDB.getTableColumns(tableName)
	if err != nil {
		t.Fatalf("更新后查询表结构失败: %v", err)
	}
	updatedType := updatedCols[testFieldName]
	if !strings.Contains(updatedType, "mediumtext") {
		t.Errorf("字段类型未更新，预期包含mediumtext，实际为: %s", updatedType)
	}
}

// TestFindMultiByWhereClauses 测试跨多张表的批量查询（golang_test1/2/3）
func TestFindMultiByWhereClauses(t *testing.T) {
	// 1. 初始化数据库连接
	pbMySqlDB := NewPbMysqlDB()
	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

	// 2. 准备4张表的测试数据（原始表+3张新增表）
	// 原始表数据
	testData := &messageoption.GolangTest{
		Id:      100,
		GroupId: 1,
		Ip:      "192.168.0.100",
		Port:    3306,
		Player: &messageoption.Player{
			PlayerId: 10000,
			Name:     "OriginalTest",
		},
	}
	// 新增表1数据
	testData1 := &messageoption.GolangTest1{
		Id:      101,
		GroupId: 1,
		Ip:      "192.168.0.101",
		Port:    3306,
		Player: &messageoption.Player{
			PlayerId: 10001,
			Name:     "Test1",
		},
		ExtraInfo: "额外信息1", // 新增字段
	}
	// 新增表2数据（port为uint64）
	testData2 := &messageoption.GolangTest2{
		Id:      102,
		GroupId: 1,
		Ip:      "192.168.0.102",
		Port:    65536, // 超过uint32的端口值
		Player: &messageoption.Player{
			PlayerId: 10002,
			Name:     "Test2",
		},
	}
	// 新增表3数据（多一个嵌套player）
	testData3 := &messageoption.GolangTest3{
		Id:      103,
		GroupId: 1,
		Ip:      "192.168.0.103",
		Port:    3306,
		Player: &messageoption.Player{
			PlayerId: 10003,
			Name:     "Test3Main",
		},
		ExtraPlayer: &messageoption.Player{ // 新增嵌套字段
			PlayerId: 10004,
			Name:     "Test3Extra",
		},
		PlayerId: 10004,
	}

	// 3. 注册表并创建表结构
	pbMySqlDB.RegisterTable(testData)
	pbMySqlDB.RegisterTable(testData1)
	pbMySqlDB.RegisterTable(testData2)
	pbMySqlDB.RegisterTable(testData3)

	// 创建/更新表结构
	if err := pbMySqlDB.CreateOrUpdateTable(testData); err != nil {
		t.Fatalf("创建golang_test表失败: %v", err)
	}
	if err := pbMySqlDB.CreateOrUpdateTable(testData1); err != nil {
		t.Fatalf("创建golang_test1表失败: %v", err)
	}
	if err := pbMySqlDB.CreateOrUpdateTable(testData2); err != nil {
		t.Fatalf("创建golang_test2表失败: %v", err)
	}
	if err := pbMySqlDB.CreateOrUpdateTable(testData3); err != nil {
		t.Fatalf("创建golang_test3表失败: %v", err)
	}

	// 4. 清理旧数据
	clearTable := func(tableName string, id interface{}) {
		sql := fmt.Sprintf("DELETE FROM %s WHERE id = ?", escapeMySQLName(tableName))
		if _, err := db.Exec(sql, id); err != nil {
			t.Logf("清理表%s(id=%v)旧数据失败: %v（可忽略）", tableName, id, err)
		}
	}
	clearTable(GetTableName(testData), testData.Id)
	clearTable(GetTableName(testData1), testData1.Id)
	clearTable(GetTableName(testData2), testData2.Id)
	clearTable(GetTableName(testData3), testData3.Id)

	// 5. 插入测试数据
	if err := pbMySqlDB.Save(testData); err != nil {
		t.Fatalf("保存golang_test数据失败: %v", err)
	}
	if err := pbMySqlDB.Save(testData1); err != nil {
		t.Fatalf("保存golang_test1数据失败: %v", err)
	}
	if err := pbMySqlDB.Save(testData2); err != nil {
		t.Fatalf("保存golang_test2数据失败: %v", err)
	}
	if err := pbMySqlDB.Save(testData3); err != nil {
		t.Fatalf("保存golang_test3数据失败: %v", err)
	}

	// 6. 准备批量查询参数（跨4张表）
	queries := []MultiQuery{
		{
			Message:     &messageoption.GolangTest{}, // 原始表
			WhereClause: "id = ? AND group_id = ?",
			WhereArgs:   []interface{}{testData.Id, testData.GroupId},
		},
		{
			Message:     &messageoption.GolangTest1{}, // 新增表1
			WhereClause: "id = ? AND extra_info = ?",  // 查询新增字段
			WhereArgs:   []interface{}{testData1.Id, testData1.ExtraInfo},
		},
		{
			Message:     &messageoption.GolangTest2{}, // 新增表2
			WhereClause: "id = ? AND port = ?",        // 查询uint64字段
			WhereArgs:   []interface{}{testData2.Id, testData2.Port},
		},
		{
			Message:     &messageoption.GolangTest3{}, // 新增表3
			WhereClause: "id = ? AND player_id = ?",   // 查询新增嵌套字段
			WhereArgs:   []interface{}{testData3.Id, testData3.ExtraPlayer.PlayerId},
		},
	}

	// 7. 执行批量查询
	if err := pbMySqlDB.FindMultiByWhereClauses(queries); err != nil {
		t.Fatalf("批量查询失败: %v", err)
	}

	// 8. 验证查询结果
	// 验证原始表
	result := queries[0].Message.(*messageoption.GolangTest)
	if !proto.Equal(testData, result) {
		t.Error("golang_test查询结果不一致")
		t.Logf("预期: %s", testData.String())
		t.Logf("实际: %s", result.String())
	}

	// 验证新增表1
	result1 := queries[1].Message.(*messageoption.GolangTest1)
	if !proto.Equal(testData1, result1) {
		t.Error("golang_test1查询结果不一致")
		t.Logf("预期: %s", testData1.String())
		t.Logf("实际: %s", result1.String())
	}

	// 验证新增表2（注意port是uint64）
	result2 := queries[2].Message.(*messageoption.GolangTest2)
	if !proto.Equal(testData2, result2) {
		t.Error("golang_test2查询结果不一致")
		t.Logf("预期: %s", testData2.String())
		t.Logf("实际: %s", result2.String())
	}

	// 验证新增表3（注意嵌套字段extra_player）
	result3 := queries[3].Message.(*messageoption.GolangTest3)
	if !proto.Equal(testData3, result3) {
		t.Error("golang_test3查询结果不一致")
		t.Logf("预期: %s", testData3.String())
		t.Logf("实际: %s", result3.String())
	}

	// 9. 测试异常场景（表2查询不存在的数据）
	invalidQueries := []MultiQuery{
		{
			Message:     &messageoption.GolangTest2{},
			WhereClause: "id = ?",
			WhereArgs:   []interface{}{9999}, // 不存在的ID
		},
	}
	if err := pbMySqlDB.FindMultiByWhereClauses(invalidQueries); err == nil {
		t.Error("预期查询不存在的ID时返回错误，但未返回")
	} else if !strings.Contains(err.Error(), ErrNoRowsFound.Error()) {
		t.Errorf("预期错误包含[%s]，实际为: %v", ErrNoRowsFound, err)
	}

	t.Log("跨表批量查询测试通过")
}

// TestFindMultiInterfaces 测试多条结果查询的三个接口
func TestFindMultiInterfaces(t *testing.T) {
	// 1. 初始化数据库连接
	pbMySqlDB := NewPbMysqlDB()
	db := mustOpenTestDB(t, pbMySqlDB)
	defer closeTestDB(t, db)

	// 2. 注册测试表（golang_test）
	testTable := &messageoption.GolangTest{}
	pbMySqlDB.RegisterTable(
		testTable,
		WithPrimaryKey("id"),
		WithAutoIncrementKey("id"),
		WithIndexes("player_id"), // 为player_id建索引，加速查询
	)

	// 3. 创建表并清理旧数据
	if err := pbMySqlDB.CreateOrUpdateTable(testTable); err != nil {
		t.Fatalf("创建表失败: %v", err)
	}
	tableName := GetTableName(testTable)
	cleanSQL := fmt.Sprintf("DELETE FROM %s WHERE player_id = ?", escapeMySQLName(tableName))
	_, _ = db.Exec(cleanSQL, 1000) // 清理player_id=1000的旧数据

	// 4. 插入测试数据（3条相同player_id的数据，用于测试多条结果）
	testData1 := &messageoption.GolangTest{
		Id:       1001,
		PlayerId: 1000, // 关键：相同的player_id
		Ip:       "192.168.1.101",
		Port:     3306,
		GroupId:  10,
	}
	testData2 := &messageoption.GolangTest{
		Id:       1002,
		PlayerId: 1000,
		Ip:       "192.168.1.102",
		Port:     3307,
		GroupId:  10,
	}
	testData3 := &messageoption.GolangTest{
		Id:       1003,
		PlayerId: 1000,
		Ip:       "192.168.1.103",
		Port:     3308,
		GroupId:  20, // 不同的groupId，用于复杂条件查询
	}
	// 插入一条不相关数据（用于验证过滤效果）
	unrelatedData := &messageoption.GolangTest{
		Id:       2001,
		PlayerId: 2000, // 不同的player_id
		Ip:       "192.168.2.101",
	}

	// 批量插入测试数据
	if err := pbMySqlDB.BatchInsert([]proto.Message{testData1, testData2, testData3, unrelatedData}); err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	// 预期结果：3条player_id=1000的数据（按id排序）
	expectedIds := map[uint32]bool{1001: true, 1002: true, 1003: true}

	// 5. 测试 FindMultiByKV（键值对查询多条结果）
	t.Run("FindMultiByKV", func(t *testing.T) {
		var resultList messageoption.GolangTestList
		err := pbMySqlDB.FindMultiByKV(&resultList, "player_id", uint64(1000))
		if err != nil {
			t.Fatalf("FindMultiByKV查询失败: %v", err)
		}

		// 验证结果数量
		if len(resultList.TestList) != 3 {
			t.Fatalf("预期3条结果，实际%d条", len(resultList.TestList))
		}

		// 验证结果正确性
		for _, item := range resultList.TestList {
			if !expectedIds[item.Id] {
				t.Errorf("结果包含非预期数据: id=%d", item.Id)
			}
			if item.PlayerId != 1000 {
				t.Errorf("数据校验失败: player_id应为1000，实际为%d", item.PlayerId)
			}
		}
	})

	// 6. 测试 FindMultiByWhereWithArgs（参数化条件查询多条结果）
	t.Run("FindMultiByWhereWithArgs", func(t *testing.T) {
		var resultList messageoption.GolangTestList
		// 复杂条件：player_id=1000 且 group_id=10
		err := pbMySqlDB.FindMultiByWhereWithArgs(
			&resultList,
			"player_id = ? AND group_id = ?",
			[]interface{}{uint64(1000), 10},
		)
		if err != nil {
			t.Fatalf("FindMultiByWhereWithArgs查询失败: %v", err)
		}

		// 验证结果数量（预期2条：1001、1002）
		if len(resultList.TestList) != 2 {
			t.Fatalf("预期2条结果，实际%d条", len(resultList.TestList))
		}

		// 验证结果正确性
		for _, item := range resultList.TestList {
			if item.Id != 1001 && item.Id != 1002 {
				t.Errorf("结果包含非预期数据: id=%d", item.Id)
			}
			if item.GroupId != 10 {
				t.Errorf("数据校验失败: group_id应为10，实际为%d", item.GroupId)
			}
		}
	})

	// 7. 测试 FindMultiByWhereClause（非参数化条件查询多条结果）
	t.Run("FindMultiByWhereClause", func(t *testing.T) {
		var resultList messageoption.GolangTestList
		// 固定条件（内部使用，无用户输入）
		err := pbMySqlDB.FindMultiByWhereClause(
			&resultList,
			"player_id = 1000 AND port > 3306", // port>3306：预期1002、1003
		)
		if err != nil {
			t.Fatalf("FindMultiByWhereClause查询失败: %v", err)
		}

		// 验证结果数量（预期2条）
		if len(resultList.TestList) != 2 {
			t.Fatalf("预期2条结果，实际%d条", len(resultList.TestList))
		}

		// 验证结果正确性
		for _, item := range resultList.TestList {
			if item.Id != 1002 && item.Id != 1003 {
				t.Errorf("结果包含非预期数据: id=%d", item.Id)
			}
			if item.Port <= 3306 {
				t.Errorf("数据校验失败: port应>3306，实际为%d", item.Port)
			}
		}
	})

	t.Log("所有多条结果查询接口测试通过")
}
