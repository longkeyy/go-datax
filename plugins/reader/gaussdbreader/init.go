package gaussdbreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// GaussDBReaderJobFactory 实现ReaderJobFactory接口
type GaussDBReaderJobFactory struct{}

func (f *GaussDBReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewGaussDBReaderJob()
}

// GaussDBReaderTaskFactory 实现ReaderTaskFactory接口
type GaussDBReaderTaskFactory struct{}

func (f *GaussDBReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewGaussDBReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("gaussdbreader", &GaussDBReaderJobFactory{})
	registry.RegisterReaderTask("gaussdbreader", &GaussDBReaderTaskFactory{})
}