package sybasewriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// SybaseWriterJobFactory 实现WriterJobFactory接口
type SybaseWriterJobFactory struct{}

func (f *SybaseWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewSybaseWriterJob()
}

// SybaseWriterTaskFactory 实现WriterTaskFactory接口
type SybaseWriterTaskFactory struct{}

func (f *SybaseWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewSybaseWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("sybasewriter", &SybaseWriterJobFactory{})
	registry.RegisterWriterTask("sybasewriter", &SybaseWriterTaskFactory{})
}