package sqlserverreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterReaderJob("sqlserverreader", func() plugin.ReaderJob {
		return NewSQLServerReaderJob()
	})

	plugin.RegisterReaderTask("sqlserverreader", func() plugin.ReaderTask {
		return NewSQLServerReaderTask()
	})
}