package sqlserverwriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// SQLServerWriterJobFactory 实现WriterJobFactory接口
type SQLServerWriterJobFactory struct{}

func (f *SQLServerWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewSQLServerWriterJob()
}

// SQLServerWriterTaskFactory 实现WriterTaskFactory接口
type SQLServerWriterTaskFactory struct{}

func (f *SQLServerWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewSQLServerWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("sqlserverwriter", &SQLServerWriterJobFactory{})
	registry.RegisterWriterTask("sqlserverwriter", &SQLServerWriterTaskFactory{})
}