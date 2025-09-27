package txtfilereader

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterReaderJob("txtfilereader", func() plugin.ReaderJob {
		return NewTxtFileReaderJob()
	})

	plugin.RegisterReaderTask("txtfilereader", func() plugin.ReaderTask {
		return NewTxtFileReaderTask()
	})
}