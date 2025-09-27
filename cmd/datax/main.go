package main

import (
	"github.com/longkeyy/go-datax/core/engine"

	// 导入插件以触发注册
	_ "github.com/longkeyy/go-datax/plugins/reader/postgresqlreader"
	_ "github.com/longkeyy/go-datax/plugins/writer/postgresqlwriter"
)

func main() {
	engine.Main()
}