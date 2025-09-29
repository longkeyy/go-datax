package ftpreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// FtpReaderJobFactory 实现ReaderJobFactory接口
type FtpReaderJobFactory struct{}

func (f *FtpReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewFtpReaderJob()
}

// FtpReaderTaskFactory 实现ReaderTaskFactory接口
type FtpReaderTaskFactory struct{}

func (f *FtpReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewFtpReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("ftpreader", &FtpReaderJobFactory{})
	registry.RegisterReaderTask("ftpreader", &FtpReaderTaskFactory{})
}