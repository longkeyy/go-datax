package elasticsearchwriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// ElasticSearchWriterJobFactory 实现WriterJobFactory接口
type ElasticSearchWriterJobFactory struct{}

func (f *ElasticSearchWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewElasticSearchWriterJob()
}

// ElasticSearchWriterTaskFactory 实现WriterTaskFactory接口
type ElasticSearchWriterTaskFactory struct{}

func (f *ElasticSearchWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewElasticSearchWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("elasticsearchwriter", &ElasticSearchWriterJobFactory{})
	registry.RegisterWriterTask("elasticsearchwriter", &ElasticSearchWriterTaskFactory{})
}