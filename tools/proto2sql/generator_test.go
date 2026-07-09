package proto2sql

import (
	"context"
	"os/exec"
	"strings"
	"testing"
)

// protooptionDir 通过模块图定位 protooption 源码目录（其中含 proto_option.proto / descriptor.proto）。
func protooptionDir(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/luyuancpp/protooption").Output()
	if err != nil {
		t.Skipf("无法定位 protooption 模块（跳过）: %v", err)
	}
	dir := strings.TrimSpace(string(out))
	if dir == "" {
		t.Skip("protooption 模块目录为空，跳过")
	}
	return dir
}

func TestGenerate(t *testing.T) {
	tables, err := Generate(context.Background(), Config{
		ProtoFiles:  []string{"testdata/account.proto"},
		ImportPaths: []string{protooptionDir(t)},
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
		ImportPaths: []string{protooptionDir(t)},
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
