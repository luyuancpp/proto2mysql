// Package proto2sql 解析 .proto 源文件（读取 protooption 的建表相关 message option），
// 复用 github.com/luyuancpp/proto2mysql 的 SQL 生成内核，产出 CREATE TABLE 语句。
//
// 与 proto2mysql 的分工：proto2mysql 是运行时库（吃编译好的 Go proto 类型、连库执行）；
// 本模块是构建期/离线工具（吃 .proto 源文件、产出 .sql 文件），依赖单向（proto2sql -> proto2mysql），
// 类型映射规则只有一份、不会漂移。
package proto2sql

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/luyuancpp/proto2mysql"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// protooption 中建表相关 message option 的字段号（见 protooption/proto_option.proto）。
// 这里按字段号反射读取，避免依赖 messageoption 的 Go 生成类型，也规避动态扩展与生成扩展的类型标识差异。
const (
	fieldOptionTableID          = 500000 // OptionTableId
	fieldOptionTableName        = 500001 // OptionTableName
	fieldOptionPrimaryKey       = 500002 // OptionPrimaryKey
	fieldOptionAutoIncrementKey = 500006 // OptionAutoIncrementKey
	fieldOptionIndex            = 500011 // OptionIndex
	fieldOptionUniqueKey        = 500012 // OptionUniqueKey
)

// Config 生成配置。
type Config struct {
	// ProtoFiles 要编译的 .proto 文件（可为相对/绝对路径；其所在目录会自动加入 import 搜索路径）。
	ProtoFiles []string
	// ImportPaths 额外的 import 搜索目录（用于定位 proto_option.proto / descriptor.proto 等被 import 的文件）。
	ImportPaths []string
	// Drop 为 true 时，在每条 CREATE TABLE 前加 DROP TABLE IF EXISTS。
	Drop bool
}

// Table 单张表的生成结果。
type Table struct {
	Name string // 表名（来自 OptionTableName）
	SQL  string // 该表的建表 SQL（含末尾分号；Drop 开启时含前置 DROP 语句）
}

// tableMeta 从 message option 中读出的建表元数据。
type tableMeta struct {
	hasTable      bool
	tableName     string
	primaryKey    string
	autoIncrement string
	index         string
	uniqueKey     string
}

// Generate 编译 cfg.ProtoFiles，为每个带 OptionTableName 的 message 生成建表 SQL，按表名排序返回。
func Generate(ctx context.Context, cfg Config) ([]Table, error) {
	filenames, importPaths := resolveInputs(cfg.ProtoFiles, cfg.ImportPaths)

	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			ImportPaths: importPaths,
		}),
		SourceInfoMode: protocompile.SourceInfoStandard,
	}

	files, err := compiler.Compile(ctx, filenames...)
	if err != nil {
		return nil, fmt.Errorf("compile proto: %w", err)
	}

	var tables []Table
	seen := make(map[string]bool)
	for _, f := range files {
		collectTables(f.Messages(), cfg.Drop, seen, &tables)
	}

	sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })
	return tables, nil
}

// collectTables 遍历消息（含嵌套消息），把带表选项的消息生成建表 SQL 追加到 out。
func collectTables(msgs protoreflect.MessageDescriptors, drop bool, seen map[string]bool, out *[]Table) {
	for i := 0; i < msgs.Len(); i++ {
		md := msgs.Get(i)

		meta := readTableMeta(md)
		if meta.hasTable && !seen[meta.tableName] {
			seen[meta.tableName] = true
			*out = append(*out, Table{Name: meta.tableName, SQL: buildTableSQL(md, meta, drop)})
		}

		// 递归嵌套消息（跳过 map entry 合成类型）
		collectTables(md.Messages(), drop, seen, out)
	}
}

// readTableMeta 反射读取 message option 中的建表元数据。
func readTableMeta(md protoreflect.MessageDescriptor) tableMeta {
	var meta tableMeta
	opts := md.Options()
	if opts == nil {
		return meta
	}
	opts.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch fd.Number() {
		case fieldOptionTableName:
			meta.tableName = v.String()
			meta.hasTable = true
		case fieldOptionPrimaryKey:
			meta.primaryKey = v.String()
		case fieldOptionAutoIncrementKey:
			meta.autoIncrement = v.String()
		case fieldOptionIndex:
			meta.index = v.String()
		case fieldOptionUniqueKey:
			meta.uniqueKey = v.String()
		}
		return true
	})
	return meta
}

// buildTableSQL 用 proto2mysql 的 SQL 生成内核产出建表语句。
func buildTableSQL(md protoreflect.MessageDescriptor, meta tableMeta, drop bool) string {
	msg := dynamicpb.NewMessage(md)
	sql := proto2mysql.GenerateCreateTableSQL(msg, buildTableOptions(meta)...)
	if drop {
		sql = fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n%s", strings.ReplaceAll(meta.tableName, "`", "``"), sql)
	}
	return sql
}

// buildTableOptions 把元数据转换为 proto2mysql 的 TableOption。
func buildTableOptions(meta tableMeta) []proto2mysql.TableOption {
	var opts []proto2mysql.TableOption
	if meta.tableName != "" {
		opts = append(opts, proto2mysql.WithTableName(meta.tableName))
	}
	if cols := splitCSV(meta.primaryKey); len(cols) > 0 {
		opts = append(opts, proto2mysql.WithPrimaryKey(cols...))
	}
	if meta.index != "" {
		opts = append(opts, proto2mysql.WithIndexes(meta.index))
	}
	if meta.uniqueKey != "" {
		opts = append(opts, proto2mysql.WithUniqueKey(meta.uniqueKey))
	}
	if meta.autoIncrement != "" {
		opts = append(opts, proto2mysql.WithAutoIncrementKey(meta.autoIncrement))
	}
	return opts
}

// resolveInputs 把 proto 文件路径归一化为“相对 import 路径的文件名 + 搜索目录”。
func resolveInputs(protoFiles, importPaths []string) (filenames, paths []string) {
	paths = append(paths, importPaths...)
	seenDir := make(map[string]bool)
	for _, p := range paths {
		seenDir[p] = true
	}
	for _, pf := range protoFiles {
		dir := filepath.Dir(pf)
		if !seenDir[dir] {
			paths = append(paths, dir)
			seenDir[dir] = true
		}
		filenames = append(filenames, filepath.Base(pf))
	}
	return filenames, paths
}

// splitCSV 拆分逗号分隔字段并去空白，忽略空项。
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
