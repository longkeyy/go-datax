package plugin

import (
	"fmt"
	"sync"

	"github.com/longkeyy/go-datax/common/plugin"
)

// 全局插件注册表实例
var GlobalRegistry plugin.PluginRegistry

func init() {
	GlobalRegistry = NewPluginRegistry()
}

// 便捷函数，用于直接从全局注册表获取工厂
func GetReaderJobFactory(name string) (plugin.ReaderJobFactory, error) {
	return GlobalRegistry.GetReaderJob(name)
}

func GetReaderTaskFactory(name string) (plugin.ReaderTaskFactory, error) {
	return GlobalRegistry.GetReaderTask(name)
}

func GetWriterJobFactory(name string) (plugin.WriterJobFactory, error) {
	return GlobalRegistry.GetWriterJob(name)
}

func GetWriterTaskFactory(name string) (plugin.WriterTaskFactory, error) {
	return GlobalRegistry.GetWriterTask(name)
}

// DefaultPluginRegistry 默认插件注册表实现
type DefaultPluginRegistry struct {
	readerJobs   map[string]plugin.ReaderJobFactory
	readerTasks  map[string]plugin.ReaderTaskFactory
	writerJobs   map[string]plugin.WriterJobFactory
	writerTasks  map[string]plugin.WriterTaskFactory
	mutex        sync.RWMutex
}

func NewPluginRegistry() plugin.PluginRegistry {
	return &DefaultPluginRegistry{
		readerJobs:  make(map[string]plugin.ReaderJobFactory),
		readerTasks: make(map[string]plugin.ReaderTaskFactory),
		writerJobs:  make(map[string]plugin.WriterJobFactory),
		writerTasks: make(map[string]plugin.WriterTaskFactory),
	}
}

func (r *DefaultPluginRegistry) RegisterReaderJob(name string, factory plugin.ReaderJobFactory) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.readerJobs[name]; exists {
		return fmt.Errorf("reader job '%s' already registered", name)
	}
	r.readerJobs[name] = factory
	return nil
}

func (r *DefaultPluginRegistry) RegisterReaderTask(name string, factory plugin.ReaderTaskFactory) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.readerTasks[name]; exists {
		return fmt.Errorf("reader task '%s' already registered", name)
	}
	r.readerTasks[name] = factory
	return nil
}

func (r *DefaultPluginRegistry) RegisterWriterJob(name string, factory plugin.WriterJobFactory) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.writerJobs[name]; exists {
		return fmt.Errorf("writer job '%s' already registered", name)
	}
	r.writerJobs[name] = factory
	return nil
}

func (r *DefaultPluginRegistry) RegisterWriterTask(name string, factory plugin.WriterTaskFactory) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.writerTasks[name]; exists {
		return fmt.Errorf("writer task '%s' already registered", name)
	}
	r.writerTasks[name] = factory
	return nil
}

func (r *DefaultPluginRegistry) GetReaderJob(name string) (plugin.ReaderJobFactory, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	factory, exists := r.readerJobs[name]
	if !exists {
		return nil, fmt.Errorf("reader job '%s' not found", name)
	}
	return factory, nil
}

func (r *DefaultPluginRegistry) GetReaderTask(name string) (plugin.ReaderTaskFactory, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	factory, exists := r.readerTasks[name]
	if !exists {
		return nil, fmt.Errorf("reader task '%s' not found", name)
	}
	return factory, nil
}

func (r *DefaultPluginRegistry) GetWriterJob(name string) (plugin.WriterJobFactory, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	factory, exists := r.writerJobs[name]
	if !exists {
		return nil, fmt.Errorf("writer job '%s' not found", name)
	}
	return factory, nil
}

func (r *DefaultPluginRegistry) GetWriterTask(name string) (plugin.WriterTaskFactory, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	factory, exists := r.writerTasks[name]
	if !exists {
		return nil, fmt.Errorf("writer task '%s' not found", name)
	}
	return factory, nil
}

func (r *DefaultPluginRegistry) ListReaderPlugins() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	var plugins []string
	for name := range r.readerJobs {
		plugins = append(plugins, name)
	}
	for name := range r.readerTasks {
		// 避免重复
		found := false
		for _, existing := range plugins {
			if existing == name {
				found = true
				break
			}
		}
		if !found {
			plugins = append(plugins, name)
		}
	}
	return plugins
}

func (r *DefaultPluginRegistry) ListWriterPlugins() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	var plugins []string
	for name := range r.writerJobs {
		plugins = append(plugins, name)
	}
	for name := range r.writerTasks {
		// 避免重复
		found := false
		for _, existing := range plugins {
			if existing == name {
				found = true
				break
			}
		}
		if !found {
			plugins = append(plugins, name)
		}
	}
	return plugins
}