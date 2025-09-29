package jsonfilereader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// JSONFileReaderJobFactory 实现ReaderJobFactory接口
type JSONFileReaderJobFactory struct{}

func (f *JSONFileReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewJSONFileReaderJob()
}

// JSONFileReaderTaskFactory 实现ReaderTaskFactory接口
type JSONFileReaderTaskFactory struct{}

func (f *JSONFileReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewJSONFileReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("jsonfilereader", &JSONFileReaderJobFactory{})
	registry.RegisterReaderTask("jsonfilereader", &JSONFileReaderTaskFactory{})
}