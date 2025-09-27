package oraclereader

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterReaderJob("oraclereader", func() plugin.ReaderJob {
		return NewOracleReaderJob()
	})

	plugin.RegisterReaderTask("oraclereader", func() plugin.ReaderTask {
		return NewOracleReaderTask()
	})
}