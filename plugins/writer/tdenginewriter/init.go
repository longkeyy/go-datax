package tdenginewriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// TDengineWriterJobFactory 实现WriterJobFactory接口
type TDengineWriterJobFactory struct{}

func (f *TDengineWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewTDengineWriterJob()
}

// TDengineWriterTaskFactory 实现WriterTaskFactory接口
type TDengineWriterTaskFactory struct{}

func (f *TDengineWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewTDengineWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("tdenginewriter", &TDengineWriterJobFactory{})
	registry.RegisterWriterTask("tdenginewriter", &TDengineWriterTaskFactory{})
}