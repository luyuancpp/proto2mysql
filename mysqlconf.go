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
	myCnf := mysql.NewConfig()
	myCnf.User = jsonConfig.User
	myCnf.Passwd = jsonConfig.Passwd
	myCnf.Addr = jsonConfig.Addr
	myCnf.Net = jsonConfig.Net
	myCnf.DBName = jsonConfig.DBName
	myCnf.InterpolateParams = true
	return myCnf
}
