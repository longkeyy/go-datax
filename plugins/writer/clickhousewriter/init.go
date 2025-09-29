package clickhousewriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// ClickHouseWriterJobFactory 实现WriterJobFactory接口
type ClickHouseWriterJobFactory struct{}

func (f *ClickHouseWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewClickHouseWriterJob()
}

// ClickHouseWriterTaskFactory 实现WriterTaskFactory接口
type ClickHouseWriterTaskFactory struct{}

func (f *ClickHouseWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewClickHouseWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("clickhousewriter", &ClickHouseWriterJobFactory{})
	registry.RegisterWriterTask("clickhousewriter", &ClickHouseWriterTaskFactory{})
}