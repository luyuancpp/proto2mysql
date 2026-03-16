package proto2mysql

import (
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
	cfg := mysql.NewConfig()
	cfg.User = jsonConfig.User
	cfg.Passwd = jsonConfig.Passwd
	cfg.Addr = jsonConfig.Addr
	cfg.Net = jsonConfig.Net
	cfg.DBName = jsonConfig.DBName
	cfg.Params = map[string]string{"charset": "utf8mb4"}
	cfg.ParseTime = true
	cfg.MultiStatements = true
	cfg.InterpolateParams = true
	return cfg
}
