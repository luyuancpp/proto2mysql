package proto2mysql

// 从 proto 描述符读取建表元数据（message option / field option），
// 使调用方在 .proto 里声明表配置后，RegisterTable 无需再传任何代码级 TableOption。
//
// 选项定义见本仓库 proto/proto2mysql_option.proto，运行时按字段号反射读取。

import (
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// message option 字段号
const (
	optNumTableName        = 500001 // 表名
	optNumPrimaryKey       = 500002 // 主键（逗号分隔=联合主键）
	optNumAutoIncrementKey = 500006 // 自增字段
	optNumIndex            = 500011 // 普通索引（分号分隔多个索引，每个索引内逗号分隔=联合索引）
	optNumUniqueKey        = 500012 // 唯一键（逗号分隔=联合唯一键）
)

// field option 字段号
const (
	optNumFieldNullable = 600100 // 该字段允许为 NULL
)

// file option 字段号
const (
	optNumFileDB = 500000 // 标记该 .proto 文件用于 proto2mysql 建表
)

// FileHasDBOption 判断某个 .proto 文件是否声明了文件级 db 选项
// （option (proto2mysql.db) = true;）。用于自动注册时筛选“用于建表”的文件。
func FileHasDBOption(fd protoreflect.FileDescriptor) bool {
	has := false
	rangeExtensions(fd.Options(), func(num protoreflect.FieldNumber, v protoreflect.Value) {
		if num == optNumFileDB && v.Bool() {
			has = true
		}
	})
	return has
}

// TableNameFromDescriptor 读取 message option 中的表名；第二返回值表示该消息是否声明了表选项。
func TableNameFromDescriptor(md protoreflect.MessageDescriptor) (string, bool) {
	name := ""
	rangeExtensions(md.Options(), func(num protoreflect.FieldNumber, v protoreflect.Value) {
		if num == optNumTableName {
			name = v.String()
		}
	})
	return name, name != ""
}

// TableOptionsFromDescriptor 从消息描述符读取建表配置，转换为 TableOption 列表。
// 支持的 message option：表名/主键/自增/索引/唯一键；field option：nullable。
// RegisterTable / GenerateCreateTableSQL 会自动应用这些选项，代码传入的 TableOption 优先级更高（后应用覆盖）。
func TableOptionsFromDescriptor(md protoreflect.MessageDescriptor) []TableOption {
	var opts []TableOption

	rangeExtensions(md.Options(), func(num protoreflect.FieldNumber, v protoreflect.Value) {
		switch num {
		case optNumTableName:
			if s := v.String(); s != "" {
				opts = append(opts, WithTableName(s))
			}
		case optNumPrimaryKey:
			if cols := splitOptionCSV(v.String()); len(cols) > 0 {
				opts = append(opts, WithPrimaryKey(cols...))
			}
		case optNumAutoIncrementKey:
			if s := strings.TrimSpace(v.String()); s != "" {
				opts = append(opts, WithAutoIncrementKey(s))
			}
		case optNumIndex:
			if idx := splitOptionIndexes(v.String()); len(idx) > 0 {
				opts = append(opts, WithIndexes(idx...))
			}
		case optNumUniqueKey:
			if s := strings.TrimSpace(v.String()); s != "" {
				opts = append(opts, WithUniqueKey(s))
			}
		}
	})

	var nullable []string
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		rangeExtensions(fd.Options(), func(num protoreflect.FieldNumber, v protoreflect.Value) {
			if num == optNumFieldNullable && v.Bool() {
				nullable = append(nullable, string(fd.Name()))
			}
		})
	}
	if len(nullable) > 0 {
		opts = append(opts, WithNullableFields(nullable...))
	}

	return opts
}

// rangeExtensions 遍历 options 消息上已设置的扩展字段（按字段号回调）。
// 按字段号而非扩展类型匹配：动态描述符（protocompile/dynamicpb）与生成代码的扩展类型标识不同，
// 但字段号一致。
func rangeExtensions(opts protoreflect.ProtoMessage, fn func(protoreflect.FieldNumber, protoreflect.Value)) {
	if opts == nil {
		return
	}
	m := opts.ProtoReflect()
	if !m.IsValid() {
		return
	}
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.IsExtension() {
			fn(fd.Number(), v)
		}
		return true
	})
}

// splitOptionCSV 拆分逗号分隔的字段列表并去空白，忽略空项。
func splitOptionCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// splitOptionIndexes 拆分索引选项：分号分隔多个索引，每个索引保留内部逗号（联合索引）。
// 例："last_login" → 1个索引；"player_id;zone_id,created_at" → 2个索引（第2个为联合索引）。
func splitOptionIndexes(s string) []string {
	parts := strings.Split(s, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
