package sqlitewriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// SQLiteWriterJobFactory 实现WriterJobFactory接口
type SQLiteWriterJobFactory struct{}

func (f *SQLiteWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewSQLiteWriterJob()
}

// SQLiteWriterTaskFactory 实现WriterTaskFactory接口
type SQLiteWriterTaskFactory struct{}

func (f *SQLiteWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewSQLiteWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("sqlitewriter", &SQLiteWriterJobFactory{})
	registry.RegisterWriterTask("sqlitewriter", &SQLiteWriterTaskFactory{})
}