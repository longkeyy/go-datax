package cassandrawriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// CassandraWriterJobFactory 实现WriterJobFactory接口
type CassandraWriterJobFactory struct{}

func (f *CassandraWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewCassandraWriterJob()
}

// CassandraWriterTaskFactory 实现WriterTaskFactory接口
type CassandraWriterTaskFactory struct{}

func (f *CassandraWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewCassandraWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("cassandrawriter", &CassandraWriterJobFactory{})
	registry.RegisterWriterTask("cassandrawriter", &CassandraWriterTaskFactory{})
}