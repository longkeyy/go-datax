package neo4jwriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// Neo4jWriterJobFactory 实现WriterJobFactory接口
type Neo4jWriterJobFactory struct{}

func (f *Neo4jWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewNeo4jWriterJob()
}

// Neo4jWriterTaskFactory 实现WriterTaskFactory接口
type Neo4jWriterTaskFactory struct{}

func (f *Neo4jWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewNeo4jWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("neo4jwriter", &Neo4jWriterJobFactory{})
	registry.RegisterWriterTask("neo4jwriter", &Neo4jWriterTaskFactory{})
}