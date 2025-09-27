package postgresqlwriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	// 注册PostgreSQL Writer插件
	plugin.RegisterWriterJob("postgresqlwriter", func() plugin.WriterJob {
		return NewPostgreSQLWriterJob()
	})

	plugin.RegisterWriterTask("postgresqlwriter", func() plugin.WriterTask {
		return NewPostgreSQLWriterTask()
	})
}