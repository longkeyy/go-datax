package mysqlwriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// MySQLWriterJobFactory 实现WriterJobFactory接口
type MySQLWriterJobFactory struct{}

func (f *MySQLWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewMySQLWriterJob()
}

// MySQLWriterTaskFactory 实现WriterTaskFactory接口
type MySQLWriterTaskFactory struct{}

func (f *MySQLWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewMySQLWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("mysqlwriter", &MySQLWriterJobFactory{})
	registry.RegisterWriterTask("mysqlwriter", &MySQLWriterTaskFactory{})
}