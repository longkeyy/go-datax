package oraclereader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// OracleReaderJobFactory 实现ReaderJobFactory接口
type OracleReaderJobFactory struct{}

func (f *OracleReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewOracleReaderJob()
}

// OracleReaderTaskFactory 实现ReaderTaskFactory接口
type OracleReaderTaskFactory struct{}

func (f *OracleReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewOracleReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("oraclereader", &OracleReaderJobFactory{})
	registry.RegisterReaderTask("oraclereader", &OracleReaderTaskFactory{})
}