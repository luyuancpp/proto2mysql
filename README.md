# proto2mysql

一个高效的 Go 库，用于自动将 Protobuf 消息映射到 MySQL 表结构，并提供简洁的 CRUD 操作接口，无需手动编写 SQL。

## 功能特点

- **自动映射**：Protobuf 消息与 MySQL 表结构自动映射，包括字段类型转换
- **结构管理**：自动创建和更新表结构，支持主键、索引、唯一键和自增字段
- **安全操作**：所有数据库操作使用参数化查询，避免 SQL 注入风险
- **类型处理**：内置支持 Protobuf 特殊类型（如 Timestamp）和集合类型（map/list）
- **批量操作**：支持批量插入等高效操作，提升性能
- **并发安全**：内部使用读写锁保证并发操作安全

## 安装

```bash
go get github.com/your-username/proto2mysql
```

## 快速开始

### 1. 定义 Protobuf 消息（表配置直接写在 proto 里）

```protobuf
syntax = "proto3";
package example;

import "google/protobuf/timestamp.proto";
import "proto2mysql_option.proto";  // 本仓库 proto/ 目录提供

option (proto2mysql.db) = true;  // 文件级标识：本文件用于 proto2mysql 建表（供自动扫描识别）

message User {
  option (proto2mysql.table_name)         = "user";
  option (proto2mysql.primary_key)        = "id";
  option (proto2mysql.auto_increment_key) = "id";
  option (proto2mysql.unique_key)         = "email";        // 逗号分隔 = 联合唯一键
  option (proto2mysql.index)              = "name;age";     // 分号分隔多个索引，索引内逗号 = 联合索引

  int64  id         = 1;
  string name       = 2;
  string email      = 3;
  int32  age        = 4 [(proto2mysql.nullable) = true];    // 该列允许为 NULL
  google.protobuf.Timestamp create_time = 5;
}

message UserList {
  repeated User items = 1;  // 用于批量查询
}
```

### 2. 生成 Go 代码

```bash
protoc --go_out=. --go_opt=paths=source_relative example.proto
```

### 3. 使用 proto2mysql 库

```go
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/your-username/proto2mysql"
	pb "your-module/example"
)

func main() {
	// 1. 连接 MySQL 数据库
	db, err := sql.Open("mysql", "user:password@tcp(localhost:3306)/testdb?parseTime=true")
	if err != nil {
		log.Fatalf("无法连接数据库: %v", err)
	}
	defer db.Close()

	// 2. 初始化 proto2mysql 实例
	pbDB := proto2mysql.NewDB()
	if err := pbDB.OpenDB(db, "testdb"); err != nil {
		log.Fatalf("无法打开数据库: %v", err)
	}

	// 3. 注册 Protobuf 消息与表的映射关系
	// 表配置（表名/主键/自增/索引/唯一键/可空字段）自动从 proto 的 option 中读取，无需传参；
	// 也可以传 TableOption 覆盖 proto 里的声明（proto 未声明时同样可用代码配置）：
	// pbDB.RegisterTable(&pb.User{}, proto2mysql.WithPrimaryKey("id"), ...)
	pbDB.RegisterTable(&pb.User{})

	// 4. 创建或更新表结构
	if err := pbDB.CreateOrUpdateTable(&pb.User{}); err != nil {
		log.Fatalf("创建表失败: %v", err)
	}

	// 5. 执行 CRUD 操作
	// 插入数据
	user := &pb.User{
		Name:       "张三",
		Email:      "zhangsan@example.com",
		Age:        30,
		CreateTime: timestamppb.Now(),
	}
	if err := pbDB.Insert(user); err != nil {
		log.Printf("插入失败: %v", err)
	}

	// 查询数据
	var result pb.User
	if err := pbDB.FindOneByKV(&result, "email", "zhangsan@example.com"); err != nil {
		log.Printf("查询失败: %v", err)
	} else {
		fmt.Printf("查询结果: %+v\n", result)
	}

	// 更新数据
	result.Age = 31
	if err := pbDB.Update(&result); err != nil {
		log.Printf("更新失败: %v", err)
	}

	// 批量查询
	var userList pb.UserList
	if err := pbDB.FindAllByWhereWithArgs(&userList, "age > ?", []interface{}{20}); err != nil {
		log.Printf("批量查询失败: %v", err)
	} else {
		fmt.Printf("批量查询结果: %+v\n", userList)
	}

	// 删除数据
	if err := pbDB.Delete(&result); err != nil {
		log.Printf("删除失败: %v", err)
	}
}
```

### 4. 自动注册（无需逐个 RegisterTable）

如果接入方不想手动为每个消息调用 `RegisterTable`，可以让库自动扫描项目里的
proto 描述符（descriptor option）来注册全部表。规则是：

1. `.proto` 文件顶部声明 `option (proto2mysql.db) = true;`（文件级标识：本文件用于建表）；
2. 文件里的某个 `message` 声明了 `option (proto2mysql.table_name) = "...";`。

同时满足这两点的 message 才会被自动注册；只声明了 `db` 但没有 `table_name`
的消息（如列表消息 `UserList`、内嵌子消息）会被跳过。

前提：这些 `.proto` 生成的 Go 代码已被链接进当前二进制（有任意 `import`，
其 `init` 会把描述符注册到 `protoregistry.GlobalFiles` 全局表）。

```go
pbDB := proto2mysql.NewDB()
if err := pbDB.OpenDB(db, "testdb"); err != nil {
	log.Fatalf("无法打开数据库: %v", err)
}

// 自动扫描全局描述符，注册所有“文件声明了 db 且 message 声明了 table_name”的表。
// 返回被注册的表名（proto full name）列表。
registered := pbDB.RegisterAllTables()
log.Printf("自动注册的表: %v", registered)

// 一次性对所有已注册的表建表 / 对齐字段（表不存在则创建，存在则 ALTER 对齐）。
if err := pbDB.SyncAllTables(); err != nil {
	log.Fatalf("同步表结构失败: %v", err)
}

// 之后即可直接 CRUD，无需再逐个 RegisterTable。
```

> 说明：是否建表只取决于各 `message` 是否声明了 `table_name`；文件级 `db`
> 选项只用于圈定“哪些文件参与自动扫描”，不改变单个消息的建表行为。

## 核心功能

### 表结构管理

- `RegisterTable(m proto.Message, opts ...TableOption)`: 手动注册单个消息与表的映射
- `RegisterAllTables() []string`: 自动扫描全局描述符，注册所有“文件声明了 db 且 message 声明了 table_name”的表，返回被注册的表名
- `SyncAllTables() error`: 对所有已注册的表批量建表/对齐字段
- `CreateOrUpdateTable(m proto.Message)`: 创建表（如果不存在）或更新表结构
- `UpdateTableField(m proto.Message)`: 同步表字段结构
- `IsTableExists(tableName string) (bool, error)`: 检查表是否存在

#### 按 proto 字段号（Field id）迁移，改名/改类型保留数据

建表时每列都会写入注释 `COMMENT 'pb:<字段号>'`，记录该列对应的 proto 字段号。之后调用
`UpdateTableField` / `SyncAllTables` / `GenerateMigrationSQL` 同步结构时，会先扫描线上表
（读取 `information_schema` 的列类型与注释），并按如下优先级对齐：

1. **列名相同**：类型不兼容时 `MODIFY COLUMN` 对齐类型（并回填字段号注释）；
2. **列名不同但字段号相同**（即 proto 里把该字段改了名字）：用
   `CHANGE COLUMN 旧列名 新列名 新类型 COMMENT 'pb:N'` 改名并对齐类型，**原有数据保留**；
3. **找不到对应列**：`ADD COLUMN` 新增。

> 注意：旧版本（本特性之前）建的表，列上没有 `pb:N` 注释，因此**首次**同步无法按字段号识别
> 改名（会退化为按列名匹配）。首次同步会为同名列自动回填字段号注释，之后即可正常按字段号
> 识别改名。新建的表从一开始就带注释，改名识别始终有效。
> 该逻辑位于运行时库（需连库）；离线的 `proto2sql` 工具不连库，不做此扫描。

### 数据操作

#### 插入
- `Insert(message proto.Message) error`: 插入单条记录
- `BatchInsert(messages []proto.Message) error`: 批量插入记录
- `InsertOnDupUpdate(message proto.Message) error`: 插入或更新（主键冲突时）
- `Save(message proto.Message) error`: 替换记录（基于 REPLACE 语句）

#### 查询
- `FindOneByKV(message proto.Message, whereKey string, whereVal string) error`: 按键值对查询单条记录
- `FindOneByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error`: 按条件查询单条记录
- `FindAll(message proto.Message) error`: 查询所有记录
- `FindAllByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error`: 按条件查询多条记录

#### 更新
- `Update(message proto.Message) error`: 按主键更新记录

#### 删除
- `Delete(message proto.Message) error`: 按主键删除记录

## 类型映射

| Protobuf 类型 | MySQL 类型 | 说明 |
|--------------|-----------|------|
| int32        | int NOT NULL DEFAULT 0 | - |
| uint32       | int unsigned NOT NULL DEFAULT 0 | - |
| int64        | bigint NOT NULL DEFAULT 0 | - |
| uint64       | bigint unsigned NOT NULL DEFAULT 0 | - |
| float        | float NOT NULL DEFAULT 0 | - |
| double       | double NOT NULL DEFAULT 0 | - |
| bool         | tinyint(1) NOT NULL DEFAULT 0 | - |
| string       | MEDIUMTEXT | - |
| bytes        | MEDIUMBLOB | - |
| enum         | int NOT NULL DEFAULT 0 | 存储枚举值的数字表示 |
| message      | MEDIUMBLOB | 序列化存储 |
| map          | MEDIUMBLOB | 序列化存储 |
| repeated     | MEDIUMBLOB | 序列化存储 |
| Timestamp    | DATETIME | 自动处理时间格式转换 |

## 配置选项

通过 `TableOption` 函数可以配置表的各种属性：

- `WithPrimaryKey(keys ...string)`: 设置主键字段
- `WithIndexes(indexes ...string)`: 设置普通索引
- `WithUniqueKey(uniqueKey string)`: 设置唯一键
- `WithAutoIncrementKey(key string)`: 设置自增字段
- `WithNullableFields(fields ...string)`: 设置允许为 NULL 的字段

## 注意事项

1. 批量插入的最大条数默认为 1000，可以通过修改 `BatchInsertMaxSize` 常量调整
2. Protobuf 消息中的 `repeated` 字段用于批量查询时，需要定义一个包含该字段的消息（如示例中的 `UserList`）
3. 所有字段名会自动检测是否与 MySQL 关键字冲突，冲突时会自动添加反引号包裹

## 许可证

[MIT](LICENSE)
