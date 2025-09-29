package ossreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// OssReaderJobFactory 实现ReaderJobFactory接口
type OssReaderJobFactory struct{}

func (f *OssReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewOssReaderJob()
}

// OssReaderTaskFactory 实现ReaderTaskFactory接口
type OssReaderTaskFactory struct{}

func (f *OssReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewOssReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("ossreader", &OssReaderJobFactory{})
	registry.RegisterReaderTask("ossreader", &OssReaderTaskFactory{})
}