package config

// Configuration 配置管理接口 - 纯接口定义，不包含实现
type Configuration interface {
	// 基本设置和获取
	Set(path string, value interface{})
	Get(path string) interface{}

	// 类型安全的获取方法
	GetString(path string) string
	GetStringWithDefault(path, defaultValue string) string
	GetInt(path string) int
	GetIntWithDefault(path string, defaultValue int) int
	GetLong(path string) int64
	GetLongWithDefault(path string, defaultValue int64) int64
	GetBool(path string) bool
	GetBoolWithDefault(path string, defaultValue bool) bool

	// 复合类型获取
	GetList(path string) []interface{}
	GetStringList(path string) []string
	GetStringArray(path string) []string
	GetMap(path string) map[string]interface{}

	// 嵌套配置
	GetConfiguration(path string) Configuration
	GetListConfiguration(path string) []Configuration

	// 工具方法
	ToJSON() (string, error)
	Clone() Configuration
	IsExists(path string) bool
}

// ConfigurationFactory 配置工厂接口
type ConfigurationFactory interface {
	CreateConfiguration() Configuration
	CreateConfigurationFromMap(data map[string]interface{}) Configuration
	FromJSON(jsonStr string) (Configuration, error)
	FromFile(filename string) (Configuration, error)
}