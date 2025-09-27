package postgresqlreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	// 注册PostgreSQL Reader插件
	plugin.RegisterReaderJob("postgresqlreader", func() plugin.ReaderJob {
		return NewPostgreSQLReaderJob()
	})

	plugin.RegisterReaderTask("postgresqlreader", func() plugin.ReaderTask {
		return NewPostgreSQLReaderTask()
	})
}