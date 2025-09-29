package jsonfilewriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// JSONFileWriterJobFactory 实现WriterJobFactory接口
type JSONFileWriterJobFactory struct{}

func (f *JSONFileWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewJSONFileWriterJob()
}

// JSONFileWriterTaskFactory 实现WriterTaskFactory接口
type JSONFileWriterTaskFactory struct{}

func (f *JSONFileWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewJSONFileWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("jsonfilewriter", &JSONFileWriterJobFactory{})
	registry.RegisterWriterTask("jsonfilewriter", &JSONFileWriterTaskFactory{})
}