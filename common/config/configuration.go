package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
)

// DefaultConfigurationFactory 默认配置工厂实现
type DefaultConfigurationFactory struct{}

func NewConfigurationFactory() ConfigurationFactory {
	return &DefaultConfigurationFactory{}
}

func (f *DefaultConfigurationFactory) CreateConfiguration() Configuration {
	return NewConfiguration()
}

func (f *DefaultConfigurationFactory) CreateConfigurationFromMap(data map[string]interface{}) Configuration {
	return NewConfigurationFromMap(data)
}

func (f *DefaultConfigurationFactory) FromJSON(jsonStr string) (Configuration, error) {
	return FromJSON(jsonStr)
}

func (f *DefaultConfigurationFactory) FromFile(filename string) (Configuration, error) {
	return FromFile(filename)
}

// DefaultConfiguration 默认配置实现
type DefaultConfiguration struct {
	data map[string]interface{}
}

func NewConfiguration() Configuration {
	return &DefaultConfiguration{
		data: make(map[string]interface{}),
	}
}

func NewConfigurationFromMap(data map[string]interface{}) Configuration {
	return &DefaultConfiguration{
		data: data,
	}
}

func FromJSON(jsonStr string) (Configuration, error) {
	var data map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &data)
	if err != nil {
		return nil, err
	}
	return NewConfigurationFromMap(data), nil
}

func FromFile(filename string) (Configuration, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return FromJSON(string(content))
}

func (c *DefaultConfiguration) Set(path string, value interface{}) {
	keys := strings.Split(path, ".")
	current := c.data

	for i, key := range keys {
		if i == len(keys)-1 {
			current[key] = value
		} else {
			if current[key] == nil {
				current[key] = make(map[string]interface{})
			}
			if next, ok := current[key].(map[string]interface{}); ok {
				current = next
			} else {
				current[key] = make(map[string]interface{})
				current = current[key].(map[string]interface{})
			}
		}
	}
}

func (c *DefaultConfiguration) Get(path string) interface{} {
	keys := strings.Split(path, ".")
	current := c.data

	for _, key := range keys {
		if value, ok := current[key]; ok {
			if nextMap, ok := value.(map[string]interface{}); ok {
				current = nextMap
			} else {
				return value
			}
		} else {
			return nil
		}
	}
	return current
}

func (c *DefaultConfiguration) GetString(path string) string {
	value := c.Get(path)
	if value == nil {
		return ""
	}
	if str, ok := value.(string); ok {
		return str
	}
	return fmt.Sprintf("%v", value)
}

func (c *DefaultConfiguration) GetStringWithDefault(path, defaultValue string) string {
	value := c.GetString(path)
	if value == "" {
		return defaultValue
	}
	return value
}

func (c *DefaultConfiguration) GetInt(path string) int {
	value := c.Get(path)
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return 0
}

func (c *DefaultConfiguration) GetIntWithDefault(path string, defaultValue int) int {
	value := c.GetInt(path)
	if value == 0 && c.Get(path) == nil {
		return defaultValue
	}
	return value
}

func (c *DefaultConfiguration) GetLong(path string) int64 {
	value := c.Get(path)
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
	}
	return 0
}

func (c *DefaultConfiguration) GetLongWithDefault(path string, defaultValue int64) int64 {
	value := c.GetLong(path)
	if value == 0 && c.Get(path) == nil {
		return defaultValue
	}
	return value
}

func (c *DefaultConfiguration) GetBool(path string) bool {
	value := c.Get(path)
	if value == nil {
		return false
	}

	switch v := value.(type) {
	case bool:
		return v
	case string:
		return strings.ToLower(v) == "true"
	}
	return false
}

func (c *DefaultConfiguration) GetBoolWithDefault(path string, defaultValue bool) bool {
	if c.Get(path) == nil {
		return defaultValue
	}
	return c.GetBool(path)
}

func (c *DefaultConfiguration) GetList(path string) []interface{} {
	value := c.Get(path)
	if value == nil {
		return nil
	}
	if list, ok := value.([]interface{}); ok {
		return list
	}
	return nil
}

func (c *DefaultConfiguration) GetStringList(path string) []string {
	list := c.GetList(path)
	if list == nil {
		return nil
	}

	result := make([]string, len(list))
	for i, item := range list {
		result[i] = fmt.Sprintf("%v", item)
	}
	return result
}

func (c *DefaultConfiguration) GetStringArray(path string) []string {
	return c.GetStringList(path)
}

func (c *DefaultConfiguration) GetMap(path string) map[string]interface{} {
	value := c.Get(path)
	if value == nil {
		return nil
	}
	if mapVal, ok := value.(map[string]interface{}); ok {
		return mapVal
	}
	return nil
}

func (c *DefaultConfiguration) GetConfiguration(path string) Configuration {
	value := c.Get(path)
	if value == nil {
		return NewConfiguration()
	}
	if configMap, ok := value.(map[string]interface{}); ok {
		return NewConfigurationFromMap(configMap)
	}
	return NewConfiguration()
}

func (c *DefaultConfiguration) GetListConfiguration(path string) []Configuration {
	list := c.GetList(path)
	if list == nil {
		return nil
	}

	result := make([]Configuration, 0, len(list))
	for _, item := range list {
		if configMap, ok := item.(map[string]interface{}); ok {
			result = append(result, NewConfigurationFromMap(configMap))
		}
	}
	return result
}

func (c *DefaultConfiguration) ToJSON() (string, error) {
	bytes, err := json.MarshalIndent(c.data, "", "  ")
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (c *DefaultConfiguration) Clone() Configuration {
	jsonStr, _ := c.ToJSON()
	clone, _ := FromJSON(jsonStr)
	return clone
}

func (c *DefaultConfiguration) IsExists(path string) bool {
	return c.Get(path) != nil
}