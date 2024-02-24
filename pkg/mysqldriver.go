package pkb

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
	mycnf := mysql.NewConfig()
	mycnf.User = jsonConfig.User
	mycnf.Passwd = jsonConfig.Passwd
	mycnf.Addr = jsonConfig.Addr
	mycnf.Net = jsonConfig.Net
	mycnf.DBName = jsonConfig.DBName
	return mycnf
}
