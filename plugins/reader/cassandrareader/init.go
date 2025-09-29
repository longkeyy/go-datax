package cassandrareader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// CassandraReaderJobFactory 实现ReaderJobFactory接口
type CassandraReaderJobFactory struct{}

func (f *CassandraReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewCassandraReaderJob()
}

// CassandraReaderTaskFactory 实现ReaderTaskFactory接口
type CassandraReaderTaskFactory struct{}

func (f *CassandraReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewCassandraReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("cassandrareader", &CassandraReaderJobFactory{})
	registry.RegisterReaderTask("cassandrareader", &CassandraReaderTaskFactory{})
}