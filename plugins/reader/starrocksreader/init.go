package starrocksreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// StarRocksReaderJobFactory 实现ReaderJobFactory接口
type StarRocksReaderJobFactory struct{}

func (f *StarRocksReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewStarRocksReaderJob()
}

// StarRocksReaderTaskFactory 实现ReaderTaskFactory接口
type StarRocksReaderTaskFactory struct{}

func (f *StarRocksReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewStarRocksReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("starrocksreader", &StarRocksReaderJobFactory{})
	registry.RegisterReaderTask("starrocksreader", &StarRocksReaderTaskFactory{})
}