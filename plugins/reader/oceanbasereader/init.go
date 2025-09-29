package oceanbasereader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// OceanBaseReaderJobFactory 实现ReaderJobFactory接口
type OceanBaseReaderJobFactory struct{}

func (f *OceanBaseReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewOceanBaseReaderJob()
}

// OceanBaseReaderTaskFactory 实现ReaderTaskFactory接口
type OceanBaseReaderTaskFactory struct{}

func (f *OceanBaseReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewOceanBaseReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("oceanbasereader", &OceanBaseReaderJobFactory{})
	registry.RegisterReaderTask("oceanbasereader", &OceanBaseReaderTaskFactory{})
}