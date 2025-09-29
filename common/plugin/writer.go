package plugin

import (
	"context"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
)

// Writer 基础写入插件接口
type Writer interface {
	Init(config config.Configuration) error
	Destroy() error
}

// WriterJob 写入作业接口 - 负责作业级别的操作
type WriterJob interface {
	Writer
	Split(mandatoryNumber int) ([]config.Configuration, error)
	Post() error
	Prepare() error
}

// WriterTask 写入任务接口 - 负责任务级别的数据写入
type WriterTask interface {
	Writer
	StartWrite(recordReceiver RecordReceiver) error
	Post() error
	Prepare() error
}

// WriterTaskWithContext 支持Context取消的写入任务接口
type WriterTaskWithContext interface {
	WriterTask
	StartWriteWithContext(recordReceiver RecordReceiver, ctx context.Context) error
}

// RecordReceiver 记录接收接口 - 纯接口定义
type RecordReceiver interface {
	GetFromReader() (element.Record, error)
	Shutdown() error
}