package databendwriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// DatabendWriterJobFactory 实现WriterJobFactory接口
type DatabendWriterJobFactory struct{}

func (f *DatabendWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewDatabendWriterJob()
}

// DatabendWriterTaskFactory 实现WriterTaskFactory接口
type DatabendWriterTaskFactory struct{}

func (f *DatabendWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewDatabendWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("databendwriter", &DatabendWriterJobFactory{})
	registry.RegisterWriterTask("databendwriter", &DatabendWriterTaskFactory{})
}