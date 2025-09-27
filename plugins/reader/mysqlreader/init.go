package mysqlreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterReaderJob("mysqlreader", func() plugin.ReaderJob {
		return NewMySQLReaderJob()
	})
	plugin.RegisterReaderTask("mysqlreader", func() plugin.ReaderTask {
		return NewMySQLReaderTask()
	})
}