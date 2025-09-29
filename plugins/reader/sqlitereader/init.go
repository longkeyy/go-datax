package sqlitereader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// SQLiteReaderJobFactory 实现ReaderJobFactory接口
type SQLiteReaderJobFactory struct{}

func (f *SQLiteReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewSQLiteReaderJob()
}

// SQLiteReaderTaskFactory 实现ReaderTaskFactory接口
type SQLiteReaderTaskFactory struct{}

func (f *SQLiteReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewSQLiteReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("sqlitereader", &SQLiteReaderJobFactory{})
	registry.RegisterReaderTask("sqlitereader", &SQLiteReaderTaskFactory{})
}