package clickhousereader

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterReaderJob("clickhousereader", func() plugin.ReaderJob {
		return NewClickHouseReaderJob()
	})

	plugin.RegisterReaderTask("clickhousereader", func() plugin.ReaderTask {
		return NewClickHouseReaderTask()
	})
}