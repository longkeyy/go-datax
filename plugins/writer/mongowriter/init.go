package mongowriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterWriterJob("mongowriter", func() plugin.WriterJob {
		return NewMongoWriterJob()
	})

	plugin.RegisterWriterTask("mongowriter", func() plugin.WriterTask {
		return NewMongoWriterTask()
	})
}