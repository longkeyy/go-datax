package dorisreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// UdorisReaderJobFactory 实现ReaderJobFactory接口
type UdorisReaderJobFactory struct{}

func (f *UdorisReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewDorisReaderJob()
}

// UdorisReaderTaskFactory 实现ReaderTaskFactory接口
type UdorisReaderTaskFactory struct{}

func (f *UdorisReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewDorisReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("dorisreader", &UdorisReaderJobFactory{})
	registry.RegisterReaderTask("dorisreader", &UdorisReaderTaskFactory{})
}
