package gaussdbwriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// GaussDBWriterJobFactory 实现WriterJobFactory接口
type GaussDBWriterJobFactory struct{}

func (f *GaussDBWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewGaussDBWriterJob()
}

// GaussDBWriterTaskFactory 实现WriterTaskFactory接口
type GaussDBWriterTaskFactory struct{}

func (f *GaussDBWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewGaussDBWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("gaussdbwriter", &GaussDBWriterJobFactory{})
	registry.RegisterWriterTask("gaussdbwriter", &GaussDBWriterTaskFactory{})
}