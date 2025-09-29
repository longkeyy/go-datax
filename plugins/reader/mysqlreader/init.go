package mysqlreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// MySQLReaderJobFactory 实现ReaderJobFactory接口
type MySQLReaderJobFactory struct{}

func (f *MySQLReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewMySQLReaderJob()
}

// MySQLReaderTaskFactory 实现ReaderTaskFactory接口
type MySQLReaderTaskFactory struct{}

func (f *MySQLReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewMySQLReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("mysqlreader", &MySQLReaderJobFactory{})
	registry.RegisterReaderTask("mysqlreader", &MySQLReaderTaskFactory{})
}