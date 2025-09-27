package plugin

import (
	"fmt"
)

// ReaderJobFactory Reader Job工厂函数
type ReaderJobFactory func() ReaderJob

// ReaderTaskFactory Reader Task工厂函数
type ReaderTaskFactory func() ReaderTask

// WriterJobFactory Writer Job工厂函数
type WriterJobFactory func() WriterJob

// WriterTaskFactory Writer Task工厂函数
type WriterTaskFactory func() WriterTask

// PluginRegistry 插件注册表
type PluginRegistry struct {
	readerJobs  map[string]ReaderJobFactory
	readerTasks map[string]ReaderTaskFactory
	writerJobs  map[string]WriterJobFactory
	writerTasks map[string]WriterTaskFactory
}

var globalRegistry = &PluginRegistry{
	readerJobs:  make(map[string]ReaderJobFactory),
	readerTasks: make(map[string]ReaderTaskFactory),
	writerJobs:  make(map[string]WriterJobFactory),
	writerTasks: make(map[string]WriterTaskFactory),
}

// RegisterReaderJob 注册Reader Job
func RegisterReaderJob(name string, factory ReaderJobFactory) {
	globalRegistry.readerJobs[name] = factory
}

// RegisterReaderTask 注册Reader Task
func RegisterReaderTask(name string, factory ReaderTaskFactory) {
	globalRegistry.readerTasks[name] = factory
}

// RegisterWriterJob 注册Writer Job
func RegisterWriterJob(name string, factory WriterJobFactory) {
	globalRegistry.writerJobs[name] = factory
}

// RegisterWriterTask 注册Writer Task
func RegisterWriterTask(name string, factory WriterTaskFactory) {
	globalRegistry.writerTasks[name] = factory
}

// CreateReaderJob 创建Reader Job实例
func CreateReaderJob(name string) (ReaderJob, error) {
	factory, exists := globalRegistry.readerJobs[name]
	if !exists {
		return nil, fmt.Errorf("reader job plugin not found: %s", name)
	}
	return factory(), nil
}

// CreateReaderTask 创建Reader Task实例
func CreateReaderTask(name string) (ReaderTask, error) {
	factory, exists := globalRegistry.readerTasks[name]
	if !exists {
		return nil, fmt.Errorf("reader task plugin not found: %s", name)
	}
	return factory(), nil
}

// CreateWriterJob 创建Writer Job实例
func CreateWriterJob(name string) (WriterJob, error) {
	factory, exists := globalRegistry.writerJobs[name]
	if !exists {
		return nil, fmt.Errorf("writer job plugin not found: %s", name)
	}
	return factory(), nil
}

// CreateWriterTask 创建Writer Task实例
func CreateWriterTask(name string) (WriterTask, error) {
	factory, exists := globalRegistry.writerTasks[name]
	if !exists {
		return nil, fmt.Errorf("writer task plugin not found: %s", name)
	}
	return factory(), nil
}