package sqlitewriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterWriterJob("sqlitewriter", func() plugin.WriterJob {
		return NewSQLiteWriterJob()
	})
	plugin.RegisterWriterTask("sqlitewriter", func() plugin.WriterTask {
		return NewSQLiteWriterTask()
	})
}