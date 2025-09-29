package factory

import (
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	coreplugin "github.com/longkeyy/go-datax/core/registry"
)

// DataXFactory 主工厂 - 集中管理所有工厂的创建
type DataXFactory struct {
	columnFactory             element.ColumnFactory
	recordFactory             element.RecordFactory
	configurationFactory      config.ConfigurationFactory
	channelFactory            plugin.ChannelFactory
	senderReceiverFactory     plugin.SenderReceiverFactory
	pluginRegistry            plugin.PluginRegistry
}

// NewDataXFactory 创建新的DataX工厂实例
func NewDataXFactory() *DataXFactory {
	return &DataXFactory{
		columnFactory:         element.NewColumnFactory(),
		recordFactory:         element.NewRecordFactory(),
		configurationFactory:  config.NewConfigurationFactory(),
		channelFactory:        coreplugin.NewChannelFactory(),
		senderReceiverFactory: coreplugin.NewSenderReceiverFactory(),
		pluginRegistry:        coreplugin.GlobalRegistry, // 使用全局注册表
	}
}

// Element factories
func (f *DataXFactory) GetColumnFactory() element.ColumnFactory {
	return f.columnFactory
}

func (f *DataXFactory) GetRecordFactory() element.RecordFactory {
	return f.recordFactory
}

// Config factory
func (f *DataXFactory) GetConfigurationFactory() config.ConfigurationFactory {
	return f.configurationFactory
}

// Plugin factories
func (f *DataXFactory) GetChannelFactory() plugin.ChannelFactory {
	return f.channelFactory
}

func (f *DataXFactory) GetSenderReceiverFactory() plugin.SenderReceiverFactory {
	return f.senderReceiverFactory
}

func (f *DataXFactory) GetPluginRegistry() plugin.PluginRegistry {
	return f.pluginRegistry
}


// 全局工厂实例 - 单例模式
var globalFactory *DataXFactory

// GetGlobalFactory 获取全局工厂实例
func GetGlobalFactory() *DataXFactory {
	if globalFactory == nil {
		globalFactory = NewDataXFactory()
	}
	return globalFactory
}

// SetGlobalFactory 设置全局工厂实例（主要用于测试）
func SetGlobalFactory(factory *DataXFactory) {
	globalFactory = factory
}