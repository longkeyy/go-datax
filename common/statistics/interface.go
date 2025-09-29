package statistics

import (
	"time"
)

// StatisticsCollector 统计信息收集器接口
type StatisticsCollector interface {
	// 记录传输统计
	RecordSentRecord(byteSize int)
	RecordFailedRecord(byteSize int, err error)
	RecordFilteredRecord(byteSize int)

	// 获取统计信息
	GetTotalRecords() int64
	GetSentRecords() int64
	GetFailedRecords() int64
	GetFilteredRecords() int64
	GetTotalBytes() int64
	GetSpeed() float64 // records per second
	GetStartTime() time.Time
	GetEndTime() time.Time
	GetElapsedTime() time.Duration

	// 重置统计
	Reset()
}

// StatisticsReporter 统计信息报告器接口
type StatisticsReporter interface {
	ReportProgress(collector StatisticsCollector)
	ReportFinal(collector StatisticsCollector)
	ReportError(collector StatisticsCollector, err error)
}

// MessageCommunicator 消息通信器接口 - 负责组件间的状态通信
type MessageCommunicator interface {
	RegisterStatisticsCollector(name string, collector StatisticsCollector)
	GetStatisticsCollector(name string) StatisticsCollector
	SendMessage(message interface{}) error
	ReceiveMessage() (interface{}, error)
}