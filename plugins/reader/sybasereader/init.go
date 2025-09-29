package sybasereader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// SybaseReaderJobFactory 实现ReaderJobFactory接口
type SybaseReaderJobFactory struct{}

func (f *SybaseReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewSybaseReaderJob()
}

// SybaseReaderTaskFactory 实现ReaderTaskFactory接口
type SybaseReaderTaskFactory struct{}

func (f *SybaseReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewSybaseReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("sybasereader", &SybaseReaderJobFactory{})
	registry.RegisterReaderTask("sybasereader", &SybaseReaderTaskFactory{})
}