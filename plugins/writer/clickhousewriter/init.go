package clickhousewriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterWriterJob("clickhousewriter", func() plugin.WriterJob {
		return NewClickHouseWriterJob()
	})

	plugin.RegisterWriterTask("clickhousewriter", func() plugin.WriterTask {
		return NewClickHouseWriterTask()
	})
}