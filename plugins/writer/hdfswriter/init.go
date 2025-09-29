package hdfswriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// HdfsWriterJobFactory 实现WriterJobFactory接口
type HdfsWriterJobFactory struct{}

func (f *HdfsWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewHdfsWriterJob()
}

// HdfsWriterTaskFactory 实现WriterTaskFactory接口
type HdfsWriterTaskFactory struct{}

func (f *HdfsWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewHdfsWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("hdfswriter", &HdfsWriterJobFactory{})
	registry.RegisterWriterTask("hdfswriter", &HdfsWriterTaskFactory{})
}