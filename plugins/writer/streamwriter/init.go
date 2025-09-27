package streamwriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterWriterJob("streamwriter", func() plugin.WriterJob {
		return NewStreamWriterJob()
	})

	plugin.RegisterWriterTask("streamwriter", func() plugin.WriterTask {
		return NewStreamWriterTask()
	})
}