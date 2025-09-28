package jsonfilereader

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterReaderJob("jsonfilereader", func() plugin.ReaderJob {
		return NewJSONFileReaderJob()
	})

	plugin.RegisterReaderTask("jsonfilereader", func() plugin.ReaderTask {
		return NewJSONFileReaderTask()
	})
}