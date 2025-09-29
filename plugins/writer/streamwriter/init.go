package streamwriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// StreamWriterJobFactory 实现WriterJobFactory接口
type StreamWriterJobFactory struct{}

func (f *StreamWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewStreamWriterJob()
}

// StreamWriterTaskFactory 实现WriterTaskFactory接口
type StreamWriterTaskFactory struct{}

func (f *StreamWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewStreamWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("streamwriter", &StreamWriterJobFactory{})
	registry.RegisterWriterTask("streamwriter", &StreamWriterTaskFactory{})
}