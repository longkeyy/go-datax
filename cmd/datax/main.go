package main

import (
	"github.com/longkeyy/go-datax/core/engine"

	// 导入插件以触发注册
	_ "github.com/longkeyy/go-datax/plugins/reader/postgresqlreader"
	_ "github.com/longkeyy/go-datax/plugins/writer/postgresqlwriter"
	_ "github.com/longkeyy/go-datax/plugins/reader/mysqlreader"
	_ "github.com/longkeyy/go-datax/plugins/writer/mysqlwriter"
	_ "github.com/longkeyy/go-datax/plugins/reader/sqlitereader"
	_ "github.com/longkeyy/go-datax/plugins/writer/sqlitewriter"
)

func main() {
	engine.Main()
}