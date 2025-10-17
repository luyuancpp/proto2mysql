是的，GitHub 项目的核心文档通常就是 `README.md`，它是用户了解项目的第一入口，需要简洁明了地展示核心功能、安装方式、使用示例和关键信息。以下是符合 GitHub 规范的 `proto2mysql` 项目 `README.md`，包含标准结构（项目介绍、特性、快速开始、接口示例、注意事项等），并适配 Markdown 格式：


```markdown
# proto2mysql

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Reference](https://pkg.go.dev/badge/github.com/your-username/proto2mysql.svg)](https://pkg.go.dev/github.com/your-username/proto2mysql)
[![Go Version](https://img.shields.io/badge/go-%3E%3D1.18-blue.svg)](https://golang.org/doc/install)

`proto2mysql` 是一个轻量级 Go 工具库，专注于 **Protobuf 消息与 MySQL 表的自动映射**，支持字段类型智能适配、数据直接存储/读取、表结构自动生成，让 Protobuf 与关系型数据库的交互更简单。


## 核心功能

- **字段自动映射**：Protobuf 类型（`int32`/`string`/`message`/`map` 等）与 MySQL 类型（`int`/`varchar`/`blob` 等）自动匹配，无需手动编写转换逻辑。
- **全量 CRUD 接口**：提供插入、更新、查询、删除等操作，支持 `INSERT ON DUPLICATE KEY UPDATE` 等高频场景。
- **表结构自动化**：根据 Protobuf 定义生成 `CREATE TABLE` SQL，字段变更时自动生成 `ALTER TABLE` 语句。
- **特殊字符兼容**：完美处理 `\0`/`\n`/emoji 等特殊字符，确保数据存/取一致性。
- **安全无注入**：所有 SQL 操作使用参数化查询，避免 SQL 注入风险。


## 安装

```bash
go get github.com/your-username/proto2mysql
```


## 快速开始

### 1. 定义 Protobuf 消息

通过自定义选项（`dbprotooption`）指定表结构元信息（主键、索引等）：

```proto
syntax = "proto3";
package example;

import "google/protobuf/descriptor.proto";

// 自定义表选项
extend google.protobuf.MessageOptions {
  string primary_key = 50001;      // 主键（多字段用逗号分隔）
  string index = 50002;            // 普通索引
  string unique_key = 50003;       // 唯一索引
  string auto_increment_key = 50004; // 自增字段
}

// 示例：用户背包数据
message UserBag {
  option (primary_key) = "bag_id,user_id"; // 复合主键
  option (index) = "user_id";              // 按用户ID查询的索引
  option (auto_increment_key) = "bag_id";  // 自增字段

  uint64 bag_id = 1;    // 背包ID（自增）
  uint64 user_id = 2;   // 用户ID
  string item_id = 3;   // 道具ID
  uint32 count = 4;     // 道具数量
  bool is_bound = 5;    // 是否绑定
  ItemMeta meta = 6;    // 嵌套消息（自动序列化）
}

// 嵌套消息：道具元数据
message ItemMeta {
  string expire_time = 1; // 过期时间
  map<string, int32> attrs = 2; // 属性字典
}
```


### 2. 生成 Go 代码

使用 `protoc` 生成 Go 代码（需安装 `protoc` 和 `protoc-gen-go`）：

```bash
protoc --go_out=. --go_opt=paths=source_relative example/user_bag.proto
```


### 3. 初始化与使用

```go
package main

import (
  "database/sql"
  "fmt"
  "github.com/go-sql-driver/mysql"
  "github.com/your-username/proto2mysql"
  "your-project-path/example" // 替换为你的 proto 生成代码路径
)

func main() {
  // 配置 MySQL 连接
  cfg := mysql.Config{
    User:      "root",
    Passwd:    "your-password",
    Addr:      "127.0.0.1:3306",
    DBName:    "test_db",
    ParseTime: true,
  }

  // 建立数据库连接
  db, err := sql.Open("mysql", cfg.FormatDSN())
  if err != nil {
    panic(err)
  }
  defer db.Close()

  // 初始化 proto2mysql 客户端
  pbDB := proto2mysql.NewPbMysqlDB()
  pbDB.RegisterTable(&example.UserBag{}) // 注册消息与表的映射
  if err := pbDB.OpenDB(db, cfg.DBName); err != nil {
    panic(err)
  }
  defer pbDB.Close()

  // 1. 创建表（首次使用时）
  createSQL := pbDB.GetCreateTableSQL(&example.UserBag{})
  if _, err := db.Exec(createSQL); err != nil {
    panic(err)
  }
  fmt.Println("表创建成功")

  // 2. 插入数据
  bag := &example.UserBag{
    BagId:    1,
    UserId:   1001,
    ItemId:   "sword_001",
    Count:    5,
    IsBound:  true,
    Meta: &example.ItemMeta{
      ExpireTime: "2024-12-31",
      Attrs:      map[string]int32{"attack": 100},
    },
  }
  if err := pbDB.InsertOnDupUpdate(bag); err != nil {
    panic(err)
  }
  fmt.Println("数据插入成功")

  // 3. 查询数据
  result := &example.UserBag{}
  if err := pbDB.FindOneByWhereWithArgs(
    result,
    "bag_id = ? AND user_id = ?",
    []interface{}{1, 1001},
  ); err != nil {
    panic(err)
  }
  fmt.Printf("查询结果: %+v\n", result)
}
```


## 核心接口示例

### 表结构操作

#### 创建表
```go
// 生成并执行 CREATE TABLE SQL
createSQL := pbDB.GetCreateTableSQL(&example.UserBag{})
_, err := db.Exec(createSQL)
```

#### 更新表字段（新增/修改）
```go
// 当 Protobuf 消息字段变更时，自动同步表结构
err := pbDB.UpdateTableField(&example.UserBag{})
```


### 数据操作

#### 插入/更新（主键冲突时更新）
```go
err := pbDB.InsertOnDupUpdate(bag) // bag 是 example.UserBag 实例
```

#### 按条件查询单条数据
```go
result := &example.UserBag{}
err := pbDB.FindOneByWhereWithArgs(
  result,
  "user_id = ? AND item_id = ?",
  []interface{}{1001, "sword_001"},
)
```

#### 批量查询
定义列表消息接收结果：
```proto
message UserBagList {
  repeated UserBag items = 1; // 批量存储 UserBag
}
```

```go
list := &example.UserBagList{}
err := pbDB.FindAllByWhereWithArgs(
  list,
  "user_id = ?",
  []interface{}{1001},
)
```

#### 按主键删除
```go
toDelete := &example.UserBag{BagId: 1, UserId: 1001} // 需包含主键
err := pbDB.Delete(toDelete)
```


## 接口列表

| 接口名称 | 功能描述 |
|---------|---------|
| `NewPbMysqlDB()` | 创建客户端实例 |
| `RegisterTable(m proto.Message)` | 注册 Protobuf 消息与表的映射 |
| `OpenDB(db *sql.DB, dbname string)` | 绑定数据库连接 |
| `Close()` | 关闭数据库连接 |
| `GetCreateTableSQL(m proto.Message)` | 生成创建表 SQL |
| `UpdateTableField(m proto.Message)` | 更新表字段 |
| `Insert(m proto.Message)` | 插入数据（主键冲突报错） |
| `InsertOnDupUpdate(m proto.Message)` | 插入/更新（主键冲突时更新） |
| `Save(m proto.Message)` | 替换数据（`REPLACE INTO`） |
| `Update(m proto.Message)` | 按主键更新 |
| `Delete(m proto.Message)` | 按主键删除 |
| `FindOneByKV(m, key, val string)` | 按键值查询单条 |
| `FindOneByWhereWithArgs(m, clause string, args []interface{})` | 自定义条件查询单条 |
| `FindAll(m proto.Message)` | 全表查询（用列表消息接收） |
| `FindAllByWhereWithArgs(m, clause string, args []interface{})` | 自定义条件批量查询 |


## 注意事项

1. **字符集**：MySQL 表需使用 `utf8mb4` 字符集，支持 emoji 和多语言字符。
2. **嵌套消息**：嵌套 `message` 会序列化为 `blob` 存储，读取时自动反序列化。
3. **主键配置**：必须通过 `primary_key` 选项定义主键，否则更新/删除接口无法工作。
4. **依赖**：Go 1.18+，`google.golang.org/protobuf` ≥ v1.28。


## 许可证

[MIT](LICENSE)
```


### 说明

这份 `README.md` 遵循 GitHub 最佳实践，包含以下特点：

1. **顶部徽章**：展示许可证、Go 文档链接、Go 版本要求，直观传递项目合法性和兼容性。
2. **简洁介绍**：一句话说明项目核心功能，让用户快速了解用途。
3. **结构化内容**：按“核心功能→安装→快速开始→接口示例→注意事项”排序，符合用户认知流程。
4. **代码示例**：提供可直接复制的代码片段，降低使用门槛。
5. **接口列表**：清晰展示核心接口及功能，方便用户快速查找。
6. **注意事项**：提前说明潜在问题（如字符集、主键配置），减少用户踩坑。

使用时只需将 `your-username` 替换为你的 GitHub 用户名，`your-project-path` 替换为实际项目路径即可。