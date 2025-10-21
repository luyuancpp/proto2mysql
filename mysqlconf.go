package proto2mysql

import (
	"fmt"
	"github.com/go-sql-driver/mysql"
)

type JsonConfig struct {
	Net    string `json:"Net"`
	Addr   string `json:"Addr"`
	User   string `json:"User"`
	Passwd string `json:"Passwd"`
	DBName string `json:"DBName"`
}

func NewMysqlConfig(jsonConfig JsonConfig) *mysql.Config {
	// 构建DSN字符串（直接包含连接参数，兼容性更好）
	dsn := fmt.Sprintf(
		"%s:%s@%s(%s)/%s?charset=utf8mb4&parseTime=true&multiStatements=true",
		jsonConfig.User,
		jsonConfig.Passwd,
		jsonConfig.Net,
		jsonConfig.Addr,
		jsonConfig.DBName,
	)

	// 解析DSN为mysql.Config
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		// 解析失败时返回默认配置（避免 panic）
		cfg = mysql.NewConfig()
		cfg.User = jsonConfig.User
		cfg.Passwd = jsonConfig.Passwd
		cfg.Addr = jsonConfig.Addr
		cfg.Net = jsonConfig.Net
		cfg.DBName = jsonConfig.DBName
	}

	// 启用参数插值（保持原有功能）
	cfg.InterpolateParams = true
	return cfg
}
