package plugin

import (
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
)

// PluginType 插件类型
type PluginType int

const (
	ReaderPlugin PluginType = iota
	WriterPlugin
)

// Reader 接口定义读取插件
type Reader interface {
	Init(config *config.Configuration) error
	Destroy() error
}

// ReaderJob 读取作业接口
type ReaderJob interface {
	Reader
	Split(adviceNumber int) ([]*config.Configuration, error)
	Post() error
}

// ReaderTask 读取任务接口
type ReaderTask interface {
	Reader
	StartRead(recordSender RecordSender) error
	Post() error
}

// Writer 接口定义写入插件
type Writer interface {
	Init(config *config.Configuration) error
	Destroy() error
}

// WriterJob 写入作业接口
type WriterJob interface {
	Writer
	Split(mandatoryNumber int) ([]*config.Configuration, error)
	Post() error
	Prepare() error
}

// WriterTask 写入任务接口
type WriterTask interface {
	Writer
	StartWrite(recordReceiver RecordReceiver) error
	Post() error
	Prepare() error
}

// RecordSender 记录发送接口
type RecordSender interface {
	SendRecord(record element.Record) error
	Flush() error
	Terminate() error
	Shutdown() error
}

// RecordReceiver 记录接收接口
type RecordReceiver interface {
	GetFromReader() (element.Record, error)
	Shutdown() error
}

// Channel 通道接口，连接Reader和Writer
type Channel interface {
	Close() error
	Push(record element.Record) error
	Pull() (element.Record, error)
	Size() int
	IsClosed() bool
}

// DefaultChannel 默认通道实现
type DefaultChannel struct {
	buffer  chan element.Record
	closed  bool
	maxSize int
}

func NewChannel(maxSize int) *DefaultChannel {
	return &DefaultChannel{
		buffer:  make(chan element.Record, maxSize),
		maxSize: maxSize,
		closed:  false,
	}
}

func (c *DefaultChannel) Close() error {
	if !c.closed {
		c.closed = true
		close(c.buffer)
	}
	return nil
}

func (c *DefaultChannel) Push(record element.Record) error {
	if c.closed {
		return ErrChannelClosed
	}
	c.buffer <- record
	return nil
}

func (c *DefaultChannel) Pull() (element.Record, error) {
	record, ok := <-c.buffer
	if !ok {
		return nil, ErrChannelClosed
	}
	return record, nil
}

func (c *DefaultChannel) Size() int {
	return len(c.buffer)
}

func (c *DefaultChannel) IsClosed() bool {
	return c.closed
}

// DefaultRecordSender 默认记录发送器
type DefaultRecordSender struct {
	channel Channel
}

func NewRecordSender(channel Channel) *DefaultRecordSender {
	return &DefaultRecordSender{channel: channel}
}

func (s *DefaultRecordSender) SendRecord(record element.Record) error {
	return s.channel.Push(record)
}

func (s *DefaultRecordSender) Flush() error {
	return nil
}

func (s *DefaultRecordSender) Terminate() error {
	return s.channel.Close()
}

func (s *DefaultRecordSender) Shutdown() error {
	return s.channel.Close()
}

// DefaultRecordReceiver 默认记录接收器
type DefaultRecordReceiver struct {
	channel Channel
}

func NewRecordReceiver(channel Channel) *DefaultRecordReceiver {
	return &DefaultRecordReceiver{channel: channel}
}

func (r *DefaultRecordReceiver) GetFromReader() (element.Record, error) {
	return r.channel.Pull()
}

func (r *DefaultRecordReceiver) Shutdown() error {
	return r.channel.Close()
}