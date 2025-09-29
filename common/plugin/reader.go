package plugin

import (
	"context"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
)

// Reader 基础读取插件接口
type Reader interface {
	Init(config config.Configuration) error
	Destroy() error
}

// ReaderJob 读取作业接口 - 负责作业级别的操作
type ReaderJob interface {
	Reader
	Split(adviceNumber int) ([]config.Configuration, error)
	Post() error
}

// ReaderTask 读取任务接口 - 负责任务级别的数据读取
type ReaderTask interface {
	Reader
	StartRead(recordSender RecordSender) error
	Post() error
}

// ReaderTaskWithContext 支持Context取消的读取任务接口
type ReaderTaskWithContext interface {
	ReaderTask
	StartReadWithContext(recordSender RecordSender, ctx context.Context) error
}

// RecordSender 记录发送接口 - 纯接口定义
type RecordSender interface {
	SendRecord(record element.Record) error
	Flush() error
	Terminate() error
	Shutdown() error
}