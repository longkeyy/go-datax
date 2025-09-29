package txtfilewriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// TxtFileWriterJobFactory 实现WriterJobFactory接口
type TxtFileWriterJobFactory struct{}

func (f *TxtFileWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewTxtFileWriterJob()
}

// TxtFileWriterTaskFactory 实现WriterTaskFactory接口
type TxtFileWriterTaskFactory struct{}

func (f *TxtFileWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewTxtFileWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("txtfilewriter", &TxtFileWriterJobFactory{})
	registry.RegisterWriterTask("txtfilewriter", &TxtFileWriterTaskFactory{})
}