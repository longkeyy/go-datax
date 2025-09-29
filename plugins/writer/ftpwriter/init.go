package ftpwriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// FtpWriterJobFactory 实现WriterJobFactory接口
type FtpWriterJobFactory struct{}

func (f *FtpWriterJobFactory) CreateWriterJob() plugin.WriterJob {
	return NewFtpWriterJob()
}

// FtpWriterTaskFactory 实现WriterTaskFactory接口
type FtpWriterTaskFactory struct{}

func (f *FtpWriterTaskFactory) CreateWriterTask() plugin.WriterTask {
	return NewFtpWriterTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterWriterJob("ftpwriter", &FtpWriterJobFactory{})
	registry.RegisterWriterTask("ftpwriter", &FtpWriterTaskFactory{})
}