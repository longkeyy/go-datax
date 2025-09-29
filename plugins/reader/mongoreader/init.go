package mongoreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// UmongoReaderJobFactory 实现ReaderJobFactory接口
type UmongoReaderJobFactory struct{}

func (f *UmongoReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewMongoReaderJob()
}

// UmongoReaderTaskFactory 实现ReaderTaskFactory接口
type UmongoReaderTaskFactory struct{}

func (f *UmongoReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewMongoReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("mongoreader", &UmongoReaderJobFactory{})
	registry.RegisterReaderTask("mongoreader", &UmongoReaderTaskFactory{})
}
