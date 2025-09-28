package jsonfilewriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterWriterJob("jsonfilewriter", func() plugin.WriterJob {
		return NewJSONFileWriterJob()
	})

	plugin.RegisterWriterTask("jsonfilewriter", func() plugin.WriterTask {
		return NewJSONFileWriterTask()
	})
}