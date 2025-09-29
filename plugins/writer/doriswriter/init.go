package doriswriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// DorisWriterJobFactory 实现WriterJobFactory接口
type DorisWriterJobFactory struct{}

func (f *DorisWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewDorisWriterJob()
}

// DorisWriterTaskFactory 实现WriterTaskFactory接口
type DorisWriterTaskFactory struct{}

func (f *DorisWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewDorisWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("doriswriter", &DorisWriterJobFactory{})
	registry.RegisterWriterTask("doriswriter", &DorisWriterTaskFactory{})
}