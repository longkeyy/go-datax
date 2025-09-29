package postgresqlreader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// PostgreSQLReaderJobFactory 实现ReaderJobFactory接口
type PostgreSQLReaderJobFactory struct{}

func (f *PostgreSQLReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewPostgreSQLReaderJob()
}

// PostgreSQLReaderTaskFactory 实现ReaderTaskFactory接口
type PostgreSQLReaderTaskFactory struct{}

func (f *PostgreSQLReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewPostgreSQLReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("postgresqlreader", &PostgreSQLReaderJobFactory{})
	registry.RegisterReaderTask("postgresqlreader", &PostgreSQLReaderTaskFactory{})
}