package dorisreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterReaderJob("dorisreader", func() plugin.ReaderJob {
		return NewDorisReaderJob()
	})

	plugin.RegisterReaderTask("dorisreader", func() plugin.ReaderTask {
		return NewDorisReaderTask()
	})
}