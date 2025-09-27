package mysqlwriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterWriterJob("mysqlwriter", func() plugin.WriterJob {
		return NewMySQLWriterJob()
	})
	plugin.RegisterWriterTask("mysqlwriter", func() plugin.WriterTask {
		return NewMySQLWriterTask()
	})
}