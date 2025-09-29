package hdfsreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// HdfsReaderJobFactory 实现ReaderJobFactory接口
type HdfsReaderJobFactory struct{}

func (f *HdfsReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewHdfsReaderJob()
}

// HdfsReaderTaskFactory 实现ReaderTaskFactory接口
type HdfsReaderTaskFactory struct{}

func (f *HdfsReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewHdfsReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("hdfsreader", &HdfsReaderJobFactory{})
	registry.RegisterReaderTask("hdfsreader", &HdfsReaderTaskFactory{})
}