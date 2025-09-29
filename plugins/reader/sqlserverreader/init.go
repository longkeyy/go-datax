package sqlserverreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// SQLServerReaderJobFactory 实现ReaderJobFactory接口
type SQLServerReaderJobFactory struct{}

func (f *SQLServerReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewSQLServerReaderJob()
}

// SQLServerReaderTaskFactory 实现ReaderTaskFactory接口
type SQLServerReaderTaskFactory struct{}

func (f *SQLServerReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewSQLServerReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("sqlserverreader", &SQLServerReaderJobFactory{})
	registry.RegisterReaderTask("sqlserverreader", &SQLServerReaderTaskFactory{})
}