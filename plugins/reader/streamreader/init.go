package streamreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterReaderJob("streamreader", func() plugin.ReaderJob {
		return NewStreamReaderJob()
	})

	plugin.RegisterReaderTask("streamreader", func() plugin.ReaderTask {
		return NewStreamReaderTask()
	})
}