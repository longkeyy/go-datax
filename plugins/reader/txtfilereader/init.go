package txtfilereader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// TxtFileReaderJobFactory 实现ReaderJobFactory接口
type TxtFileReaderJobFactory struct{}

func (f *TxtFileReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewTxtFileReaderJob()
}

// TxtFileReaderTaskFactory 实现ReaderTaskFactory接口
type TxtFileReaderTaskFactory struct{}

func (f *TxtFileReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewTxtFileReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("txtfilereader", &TxtFileReaderJobFactory{})
	registry.RegisterReaderTask("txtfilereader", &TxtFileReaderTaskFactory{})
}