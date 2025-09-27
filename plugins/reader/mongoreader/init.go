package mongoreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterReaderJob("mongoreader", func() plugin.ReaderJob {
		return NewMongoReaderJob()
	})

	plugin.RegisterReaderTask("mongoreader", func() plugin.ReaderTask {
		return NewMongoReaderTask()
	})
}