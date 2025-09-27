package transformer

import (
	"fmt"
	"strings"
	"sync"
)

// TransformerRegistry 转换器注册表
type TransformerRegistry struct {
	mutex        sync.RWMutex
	transformers map[string]*TransformerInfo
}

var globalRegistry = &TransformerRegistry{
	transformers: make(map[string]*TransformerInfo),
}

// GetGlobalRegistry 获取全局注册表实例
func GetGlobalRegistry() *TransformerRegistry {
	return globalRegistry
}

// RegisterTransformer 注册转换器
func (tr *TransformerRegistry) RegisterTransformer(transformer Transformer, isNative bool) error {
	tr.mutex.Lock()
	defer tr.mutex.Unlock()

	name := transformer.GetTransformerName()
	if name == "" {
		return fmt.Errorf("transformer name cannot be empty")
	}

	// 检查命名规范
	if err := tr.checkName(name, isNative); err != nil {
		return err
	}

	// 检查是否已注册
	if _, exists := tr.transformers[name]; exists {
		return fmt.Errorf("transformer '%s' already registered", name)
	}

	tr.transformers[name] = NewTransformerInfo(transformer, isNative)
	return nil
}

// GetTransformer 获取转换器
func (tr *TransformerRegistry) GetTransformer(name string) (*TransformerInfo, error) {
	tr.mutex.RLock()
	defer tr.mutex.RUnlock()

	if info, exists := tr.transformers[name]; exists {
		return info, nil
	}

	return nil, fmt.Errorf("transformer '%s' not found", name)
}

// GetAllTransformerNames 获取所有注册的转换器名称
func (tr *TransformerRegistry) GetAllTransformerNames() []string {
	tr.mutex.RLock()
	defer tr.mutex.RUnlock()

	names := make([]string, 0, len(tr.transformers))
	for name := range tr.transformers {
		names = append(names, name)
	}
	return names
}

// checkName 检查转换器名称规范
func (tr *TransformerRegistry) checkName(name string, isNative bool) error {
	if isNative {
		// 内置转换器必须以dx_开头
		if !strings.HasPrefix(name, "dx_") {
			return fmt.Errorf("native transformer name must start with 'dx_', got: %s", name)
		}
	} else {
		// 自定义转换器不能以dx_开头
		if strings.HasPrefix(name, "dx_") {
			return fmt.Errorf("custom transformer name cannot start with 'dx_', got: %s", name)
		}
	}
	return nil
}

// 便捷函数：注册内置转换器
func RegisterNativeTransformer(transformer Transformer) error {
	return globalRegistry.RegisterTransformer(transformer, true)
}

// 便捷函数：注册自定义转换器
func RegisterCustomTransformer(transformer Transformer) error {
	return globalRegistry.RegisterTransformer(transformer, false)
}

// 便捷函数：获取转换器
func GetTransformer(name string) (*TransformerInfo, error) {
	return globalRegistry.GetTransformer(name)
}