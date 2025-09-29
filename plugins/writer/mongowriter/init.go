package mongowriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// MongoWriterJobFactory 实现WriterJobFactory接口
type MongoWriterJobFactory struct{}

func (f *MongoWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewMongoWriterJob()
}

// MongoWriterTaskFactory 实现WriterTaskFactory接口
type MongoWriterTaskFactory struct{}

func (f *MongoWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewMongoWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("mongowriter", &MongoWriterJobFactory{})
	registry.RegisterWriterTask("mongowriter", &MongoWriterTaskFactory{})
}