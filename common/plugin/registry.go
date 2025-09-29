package plugin

// PluginType 插件类型
type PluginType int

const (
	ReaderPlugin PluginType = iota
	WriterPlugin
)

// PluginRegistry 插件注册表接口 - 负责插件的注册和获取
type PluginRegistry interface {
	RegisterReaderJob(name string, factory ReaderJobFactory) error
	RegisterReaderTask(name string, factory ReaderTaskFactory) error
	RegisterWriterJob(name string, factory WriterJobFactory) error
	RegisterWriterTask(name string, factory WriterTaskFactory) error

	GetReaderJob(name string) (ReaderJobFactory, error)
	GetReaderTask(name string) (ReaderTaskFactory, error)
	GetWriterJob(name string) (WriterJobFactory, error)
	GetWriterTask(name string) (WriterTaskFactory, error)

	ListReaderPlugins() []string
	ListWriterPlugins() []string
}

// Plugin factories - 工厂模式接口定义
type ReaderJobFactory interface {
	CreateReaderJob() ReaderJob
}

type ReaderTaskFactory interface {
	CreateReaderTask() ReaderTask
}

type WriterJobFactory interface {
	CreateWriterJob() WriterJob
}

type WriterTaskFactory interface {
	CreateWriterTask() WriterTask
}