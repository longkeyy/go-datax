package tdenginereader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// TDengineReaderJobFactory 实现ReaderJobFactory接口
type TDengineReaderJobFactory struct{}

func (f *TDengineReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewTDengineReaderJob()
}

// TDengineReaderTaskFactory 实现ReaderTaskFactory接口
type TDengineReaderTaskFactory struct{}

func (f *TDengineReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewTDengineReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("tdenginereader", &TDengineReaderJobFactory{})
	registry.RegisterReaderTask("tdenginereader", &TDengineReaderTaskFactory{})
}