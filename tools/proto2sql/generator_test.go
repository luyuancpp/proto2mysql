package proto2sql

import (
	"context"
	"strings"
	"testing"
)

// optionProtoDir 返回本仓库自带选项定义（proto2mysql_option.proto）所在目录，
// 供 protocompile 解析 account.proto 的 import。descriptor.proto 由标准 import 自动提供。
func optionProtoDir() string {
	return "../../proto"
}

func TestGenerate(t *testing.T) {
	tables, err := Generate(context.Background(), Config{
		ProtoFiles:  []string{"testdata/account.proto"},
		ImportPaths: []string{optionProtoDir()},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d: %+v", len(tables), tables)
	}
	tbl := tables[0]
	if tbl.Name != "account" {
		t.Fatalf("table name = %q, want account", tbl.Name)
	}

	sql := tbl.SQL
	checks := []string{
		"CREATE TABLE IF NOT EXISTS `account`",
		"`id` bigint unsigned NOT NULL AUTO_INCREMENT",
		"`email` MEDIUMTEXT",
		"PRIMARY KEY (`id`)",
		"INDEX `idx_account_0` (`name`)",
		"UNIQUE KEY `uk_account` (`email`)",
	}
	for _, c := range checks {
		if !strings.Contains(sql, c) {
			t.Errorf("SQL missing %q\n--- got ---\n%s", c, sql)
		}
	}
}

func TestGenerateDrop(t *testing.T) {
	tables, err := Generate(context.Background(), Config{
		ProtoFiles:  []string{"testdata/account.proto"},
		ImportPaths: []string{optionProtoDir()},
		Drop:        true,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}
	if !strings.HasPrefix(tables[0].SQL, "DROP TABLE IF EXISTS `account`;") {
		t.Errorf("expected DROP prefix, got:\n%s", tables[0].SQL)
	}
}
