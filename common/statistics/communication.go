package statistics

import (
	"time"

	"github.com/longkeyy/go-datax/common/config"
)

// StatisticsCollectorImpl 实现新API的统计收集器
type StatisticsCollectorImpl struct {
	communication *Communication
	startTime     time.Time
	endTime       time.Time
}

// NewStatisticsCollector 创建新的统计收集器
func NewStatisticsCollector() StatisticsCollector {
	return &StatisticsCollectorImpl{
		communication: NewCommunication(),
		startTime:     time.Now(),
	}
}

func (s *StatisticsCollectorImpl) RecordSentRecord(byteSize int) {
	s.communication.IncreaseCounter(READ_SUCCEED_RECORDS, 1)
	s.communication.IncreaseCounter(READ_SUCCEED_BYTES, int64(byteSize))
}

func (s *StatisticsCollectorImpl) RecordFailedRecord(byteSize int, err error) {
	s.communication.IncreaseCounter(READ_FAILED_RECORDS, 1)
	s.communication.IncreaseCounter(READ_FAILED_BYTES, int64(byteSize))
	if err != nil {
		s.communication.SetThrowable(err)
		s.communication.SetState(StateFailed)
	}
}

func (s *StatisticsCollectorImpl) RecordFilteredRecord(byteSize int) {
	s.communication.IncreaseCounter(TRANSFORMER_FILTER_RECORDS, 1)
	s.communication.IncreaseCounter("transformerFilterBytes", int64(byteSize))
}

func (s *StatisticsCollectorImpl) GetTotalRecords() int64 {
	return s.GetSentRecords() + s.GetFailedRecords() + s.GetFilteredRecords()
}

func (s *StatisticsCollectorImpl) GetSentRecords() int64 {
	return s.communication.GetLongCounter(READ_SUCCEED_RECORDS)
}

func (s *StatisticsCollectorImpl) GetFailedRecords() int64 {
	return s.communication.GetLongCounter(READ_FAILED_RECORDS)
}

func (s *StatisticsCollectorImpl) GetFilteredRecords() int64 {
	return s.communication.GetLongCounter(TRANSFORMER_FILTER_RECORDS)
}

func (s *StatisticsCollectorImpl) GetTotalBytes() int64 {
	return s.communication.GetLongCounter(READ_SUCCEED_BYTES) +
		s.communication.GetLongCounter(READ_FAILED_BYTES) +
		s.communication.GetLongCounter("transformerFilterBytes")
}

func (s *StatisticsCollectorImpl) GetSpeed() float64 {
	elapsed := s.GetElapsedTime()
	if elapsed.Seconds() <= 0 {
		return 0
	}
	return float64(s.GetTotalRecords()) / elapsed.Seconds()
}

func (s *StatisticsCollectorImpl) GetStartTime() time.Time {
	return s.startTime
}

func (s *StatisticsCollectorImpl) GetEndTime() time.Time {
	if s.endTime.IsZero() {
		return time.Now()
	}
	return s.endTime
}

func (s *StatisticsCollectorImpl) GetElapsedTime() time.Duration {
	end := s.GetEndTime()
	return end.Sub(s.startTime)
}

func (s *StatisticsCollectorImpl) Reset() {
	s.communication.Reset()
	s.startTime = time.Now()
	s.endTime = time.Time{}
}

// MessageCommunicatorImpl 实现新API的消息通信器
type MessageCommunicatorImpl struct {
	collectors map[string]StatisticsCollector
	configuration config.Configuration
}

// NewMessageCommunicator 创建新的消息通信器
func NewMessageCommunicator(config config.Configuration) MessageCommunicator {
	return &MessageCommunicatorImpl{
		collectors:    make(map[string]StatisticsCollector),
		configuration: config,
	}
}

func (c *MessageCommunicatorImpl) RegisterStatisticsCollector(name string, collector StatisticsCollector) {
	c.collectors[name] = collector
}

func (c *MessageCommunicatorImpl) GetStatisticsCollector(name string) StatisticsCollector {
	return c.collectors[name]
}

func (c *MessageCommunicatorImpl) SendMessage(message interface{}) error {
	// 实现消息发送逻辑
	return nil
}

func (c *MessageCommunicatorImpl) ReceiveMessage() (interface{}, error) {
	// 实现消息接收逻辑
	return nil, nil
}

// StatisticsReporterImpl 实现新API的统计报告器
type StatisticsReporterImpl struct {
	config config.Configuration
}

// NewStatisticsReporter 创建新的统计报告器
func NewStatisticsReporter(config config.Configuration) StatisticsReporter {
	return &StatisticsReporterImpl{config: config}
}

func (r *StatisticsReporterImpl) ReportProgress(collector StatisticsCollector) {
	// 实现进度报告逻辑
}

func (r *StatisticsReporterImpl) ReportFinal(collector StatisticsCollector) {
	// 实现最终报告逻辑
}

func (r *StatisticsReporterImpl) ReportError(collector StatisticsCollector, err error) {
	// 实现错误报告逻辑
}