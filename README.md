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

### 1. 定义 Protobuf 消息

```protobuf
syntax = "proto3";
package example;

import "google/protobuf/timestamp.proto";

message User {
  int64  id         = 1;
  string name       = 2;
  string email      = 3;
  int32  age        = 4;
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
	pbDB := proto2mysql.NewPbMysqlDB()
	if err := pbDB.OpenDB(db, "testdb"); err != nil {
		log.Fatalf("无法打开数据库: %v", err)
	}

	// 3. 注册 Protobuf 消息与表的映射关系
	pbDB.RegisterTable(
		&pb.User{},
		proto2mysql.WithPrimaryKey("id"),          // 设置主键
		proto2mysql.WithAutoIncrementKey("id"),    // 设置自增字段
		proto2mysql.WithUniqueKey("email"),        // 设置唯一键
		proto2mysql.WithIndexes("name", "age"),    // 设置普通索引
		proto2mysql.WithNullableFields("age"),     // 设置允许为 NULL 的字段
	)

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

## 核心功能

### 表结构管理

- `CreateOrUpdateTable(m proto.Message)`: 创建表（如果不存在）或更新表结构
- `UpdateTableField(m proto.Message)`: 同步表字段结构
- `IsTableExists(tableName string) (bool, error)`: 检查表是否存在

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