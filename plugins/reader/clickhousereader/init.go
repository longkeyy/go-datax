package clickhousereader

import (
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// UclickhouseReaderJobFactory 实现ReaderJobFactory接口
type UclickhouseReaderJobFactory struct{}

func (f *UclickhouseReaderJobFactory) CreateReaderJob() plugin.ReaderJob {
	return NewClickHouseReaderJob()
}

// UclickhouseReaderTaskFactory 实现ReaderTaskFactory接口
type UclickhouseReaderTaskFactory struct{}

func (f *UclickhouseReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
	return NewClickHouseReaderTask()
}

func init() {
	factoryInstance := factory.GetGlobalFactory()
	registry := factoryInstance.GetPluginRegistry()

	registry.RegisterReaderJob("clickhousereader", &UclickhouseReaderJobFactory{})
	registry.RegisterReaderTask("clickhousereader", &UclickhouseReaderTaskFactory{})
}
