package streamreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// StreamReaderJobFactory 实现ReaderJobFactory接口
type StreamReaderJobFactory struct{}

func (f *StreamReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewStreamReaderJob()
}

// StreamReaderTaskFactory 实现ReaderTaskFactory接口
type StreamReaderTaskFactory struct{}

func (f *StreamReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewStreamReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("streamreader", &StreamReaderJobFactory{})
	registry.RegisterReaderTask("streamreader", &StreamReaderTaskFactory{})
}