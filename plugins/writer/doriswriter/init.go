package doriswriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterWriterJob("doriswriter", func() plugin.WriterJob {
		return NewDorisWriterJob()
	})

	plugin.RegisterWriterTask("doriswriter", func() plugin.WriterTask {
		return NewDorisWriterTask()
	})
}