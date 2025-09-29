package plugin

import (
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/statistics"
)

// StatisticsRecordSender 带统计功能的RecordSender - 新API版本
type StatisticsRecordSender struct {
	plugin.RecordSender
	communication *statistics.Communication
	taskId        int
}

func NewStatisticsRecordSender(sender plugin.RecordSender, communication *statistics.Communication, taskId int) *StatisticsRecordSender {
	return &StatisticsRecordSender{
		RecordSender:  sender,
		communication: communication,
		taskId:        taskId,
	}
}

func (srs *StatisticsRecordSender) SendRecord(record element.Record) error {
	err := srs.RecordSender.SendRecord(record)

	if err != nil {
		// 记录失败统计
		srs.communication.IncreaseCounter(statistics.READ_FAILED_RECORDS, 1)
		srs.communication.IncreaseCounter(statistics.READ_FAILED_BYTES, int64(record.GetByteSize()))
	} else {
		// 记录成功统计
		srs.communication.IncreaseCounter(statistics.READ_SUCCEED_RECORDS, 1)
		srs.communication.IncreaseCounter(statistics.READ_SUCCEED_BYTES, int64(record.GetByteSize()))
	}

	return err
}

// StatisticsRecordReceiver 带统计功能的RecordReceiver - 新API版本
type StatisticsRecordReceiver struct {
	plugin.RecordReceiver
	communication *statistics.Communication
	taskId        int
}

func NewStatisticsRecordReceiver(receiver plugin.RecordReceiver, communication *statistics.Communication, taskId int) *StatisticsRecordReceiver {
	return &StatisticsRecordReceiver{
		RecordReceiver: receiver,
		communication:  communication,
		taskId:         taskId,
	}
}

func (srr *StatisticsRecordReceiver) GetFromReader() (element.Record, error) {
	record, err := srr.RecordReceiver.GetFromReader()

	if err != nil {
		if err != ErrChannelClosed {
			// 记录读取失败统计
			srr.communication.IncreaseCounter(statistics.WRITE_FAILED_RECORDS, 1)
		}
	} else {
		// 记录读取成功统计
		srr.communication.IncreaseCounter(statistics.WRITE_SUCCEED_RECORDS, 1)
		srr.communication.IncreaseCounter(statistics.WRITE_SUCCEED_BYTES, int64(record.GetByteSize()))
	}

	return record, err
}