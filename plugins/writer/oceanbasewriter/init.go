package oceanbasewriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// OceanBaseWriterJobFactory 实现WriterJobFactory接口
type OceanBaseWriterJobFactory struct{}

func (f *OceanBaseWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewOceanBaseWriterJob()
}

// OceanBaseWriterTaskFactory 实现WriterTaskFactory接口
type OceanBaseWriterTaskFactory struct{}

func (f *OceanBaseWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewOceanBaseWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("oceanbasewriter", &OceanBaseWriterJobFactory{})
	registry.RegisterWriterTask("oceanbasewriter", &OceanBaseWriterTaskFactory{})
}