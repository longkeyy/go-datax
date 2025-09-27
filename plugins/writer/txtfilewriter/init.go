package txtfilewriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterWriterJob("txtfilewriter", func() plugin.WriterJob {
		return NewTxtFileWriterJob()
	})

	plugin.RegisterWriterTask("txtfilewriter", func() plugin.WriterTask {
		return NewTxtFileWriterTask()
	})
}