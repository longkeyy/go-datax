package starrockswriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// StarRocksWriterJobFactory 实现WriterJobFactory接口
type StarRocksWriterJobFactory struct{}

func (f *StarRocksWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewStarRocksWriterJob()
}

// StarRocksWriterTaskFactory 实现WriterTaskFactory接口
type StarRocksWriterTaskFactory struct{}

func (f *StarRocksWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewStarRocksWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("starrockswriter", &StarRocksWriterJobFactory{})
	registry.RegisterWriterTask("starrockswriter", &StarRocksWriterTaskFactory{})
}