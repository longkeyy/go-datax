package oraclewriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// OracleWriterJobFactory 实现WriterJobFactory接口
type OracleWriterJobFactory struct{}

func (f *OracleWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewOracleWriterJob()
}

// OracleWriterTaskFactory 实现WriterTaskFactory接口
type OracleWriterTaskFactory struct{}

func (f *OracleWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewOracleWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("oraclewriter", &OracleWriterJobFactory{})
	registry.RegisterWriterTask("oraclewriter", &OracleWriterTaskFactory{})
}