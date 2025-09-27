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
	_ "github.com/longkeyy/go-datax/plugins/reader/sqlserverreader"
	_ "github.com/longkeyy/go-datax/plugins/writer/sqlserverwriter"
	_ "github.com/longkeyy/go-datax/plugins/reader/streamreader"
	_ "github.com/longkeyy/go-datax/plugins/writer/streamwriter"
	_ "github.com/longkeyy/go-datax/plugins/reader/mongoreader"
	_ "github.com/longkeyy/go-datax/plugins/writer/mongowriter"
	_ "github.com/longkeyy/go-datax/plugins/reader/dorisreader"
	_ "github.com/longkeyy/go-datax/plugins/writer/doriswriter"
	_ "github.com/longkeyy/go-datax/plugins/reader/txtfilereader"
	_ "github.com/longkeyy/go-datax/plugins/writer/txtfilewriter"
	_ "github.com/longkeyy/go-datax/plugins/reader/clickhousereader"
	_ "github.com/longkeyy/go-datax/plugins/writer/clickhousewriter"
	// _ "github.com/longkeyy/go-datax/plugins/reader/oraclereader"
	// _ "github.com/longkeyy/go-datax/plugins/writer/oraclewriter"
)

func main() {
	engine.Main()
}