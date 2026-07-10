// Package proto2sql 解析 .proto 源文件（读取本仓库 proto2mysql_option.proto 的建表相关 message option），
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

// Config 生成配置。
type Config struct {
	// ProtoFiles 要编译的 .proto 文件（可为相对/绝对路径；其所在目录会自动加入 import 搜索路径）。
	ProtoFiles []string
	// ImportPaths 额外的 import 搜索目录（用于定位被 import 的 proto 文件和 descriptor.proto 等）。
	ImportPaths []string
	// Drop 为 true 时，在每条 CREATE TABLE 前加 DROP TABLE IF EXISTS。
	Drop bool
	// RequireDBOption 为 true 时，只处理声明了文件级选项 option (proto2mysql.db) = true; 的
	// .proto 文件（与运行时 DB.RegisterAllTables 的筛选规则一致）；未声明的文件整体跳过。
	// 默认 false：处理全部输入文件里带 table_name 的消息。
	RequireDBOption bool
}

// Table 单张表的生成结果。
type Table struct {
	Name string // 表名（来自 OptionTableName）
	SQL  string // 该表的建表 SQL（含末尾分号；Drop 开启时含前置 DROP 语句）
}

// Generate 编译 cfg.ProtoFiles，为每个带 table_name 选项的 message 生成建表 SQL，按表名排序返回。
// 若 cfg.RequireDBOption 为 true，则只处理声明了文件级 option (proto2mysql.db) = true; 的文件。
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
		// 与运行时一致：开启 RequireDBOption 时，只扫描声明了 db 文件选项的文件。
		if cfg.RequireDBOption && !proto2mysql.FileHasDBOption(f) {
			continue
		}
		collectTables(f.Messages(), cfg.Drop, seen, &tables)
	}

	sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })
	return tables, nil
}

// collectTables 遍历消息（含嵌套消息），把带表选项的消息生成建表 SQL 追加到 out。
// 表配置的读取与应用全部由 proto2mysql 内核完成（TableOptionsFromDescriptor），此处只做筛选。
func collectTables(msgs protoreflect.MessageDescriptors, drop bool, seen map[string]bool, out *[]Table) {
	for i := 0; i < msgs.Len(); i++ {
		md := msgs.Get(i)

		if name, ok := proto2mysql.TableNameFromDescriptor(md); ok && !seen[name] {
			seen[name] = true
			*out = append(*out, Table{Name: name, SQL: buildTableSQL(md, name, drop)})
		}

		// 递归嵌套消息（跳过 map entry 合成类型）
		collectTables(md.Messages(), drop, seen, out)
	}
}

// buildTableSQL 用 proto2mysql 的 SQL 生成内核产出建表语句（表选项自动从描述符读取）。
func buildTableSQL(md protoreflect.MessageDescriptor, tableName string, drop bool) string {
	msg := dynamicpb.NewMessage(md)
	sql := proto2mysql.GenerateCreateTableSQL(msg)
	if drop {
		sql = fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n%s", strings.ReplaceAll(tableName, "`", "``"), sql)
	}
	return sql
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
