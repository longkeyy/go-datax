package sqlserverwriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterWriterJob("sqlserverwriter", func() plugin.WriterJob {
		return NewSQLServerWriterJob()
	})

	plugin.RegisterWriterTask("sqlserverwriter", func() plugin.WriterTask {
		return NewSQLServerWriterTask()
	})
}