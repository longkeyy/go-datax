package plugin

import (
	"github.com/longkeyy/go-datax/common/element"
)

// Channel 通道接口，连接Reader和Writer - 纯接口定义
type Channel interface {
	Close() error
	Push(record element.Record) error
	Pull() (element.Record, error)
	Size() int
	IsClosed() bool
}

// ChannelFactory 通道工厂接口
type ChannelFactory interface {
	CreateChannel(maxSize int) Channel
}

// SenderReceiverFactory 发送器接收器工厂接口
type SenderReceiverFactory interface {
	CreateRecordSender(channel Channel) RecordSender
	CreateRecordReceiver(channel Channel) RecordReceiver
}