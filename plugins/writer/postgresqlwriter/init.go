package postgresqlwriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// PostgreSQLWriterJobFactory 实现WriterJobFactory接口
type PostgreSQLWriterJobFactory struct{}

func (f *PostgreSQLWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewPostgreSQLWriterJob()
}

// PostgreSQLWriterTaskFactory 实现WriterTaskFactory接口
type PostgreSQLWriterTaskFactory struct{}

func (f *PostgreSQLWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewPostgreSQLWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("postgresqlwriter", &PostgreSQLWriterJobFactory{})
	registry.RegisterWriterTask("postgresqlwriter", &PostgreSQLWriterTaskFactory{})
}