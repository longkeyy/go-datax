package plugin

import (
	"errors"

	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
)

var (
	ErrChannelClosed = errors.New("channel is closed")
)

// DefaultChannelFactory 默认通道工厂实现
type DefaultChannelFactory struct{}

func NewChannelFactory() plugin.ChannelFactory {
	return &DefaultChannelFactory{}
}

func (f *DefaultChannelFactory) CreateChannel(maxSize int) plugin.Channel {
	return NewChannel(maxSize)
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

// DefaultSenderReceiverFactory 发送器接收器工厂实现
type DefaultSenderReceiverFactory struct{}

func NewSenderReceiverFactory() plugin.SenderReceiverFactory {
	return &DefaultSenderReceiverFactory{}
}

func (f *DefaultSenderReceiverFactory) CreateRecordSender(channel plugin.Channel) plugin.RecordSender {
	return NewRecordSender(channel)
}

func (f *DefaultSenderReceiverFactory) CreateRecordReceiver(channel plugin.Channel) plugin.RecordReceiver {
	return NewRecordReceiver(channel)
}

// DefaultRecordSender 默认记录发送器
type DefaultRecordSender struct {
	channel plugin.Channel
}

func NewRecordSender(channel plugin.Channel) *DefaultRecordSender {
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
	channel plugin.Channel
}

func NewRecordReceiver(channel plugin.Channel) *DefaultRecordReceiver {
	return &DefaultRecordReceiver{channel: channel}
}

func (r *DefaultRecordReceiver) GetFromReader() (element.Record, error) {
	return r.channel.Pull()
}

func (r *DefaultRecordReceiver) Shutdown() error {
	return r.channel.Close()
}