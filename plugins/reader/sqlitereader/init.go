package sqlitereader

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterReaderJob("sqlitereader", func() plugin.ReaderJob {
		return NewSQLiteReaderJob()
	})
	plugin.RegisterReaderTask("sqlitereader", func() plugin.ReaderTask {
		return NewSQLiteReaderTask()
	})
}