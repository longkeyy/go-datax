package plugin

import (
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/statistics"
)

// StatisticsRecordSender 带统计功能的RecordSender
type StatisticsRecordSender struct {
	RecordSender
	communication *statistics.Communication
	taskId        int
}

func NewStatisticsRecordSender(sender RecordSender, communication *statistics.Communication, taskId int) *StatisticsRecordSender {
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

// StatisticsRecordReceiver 带统计功能的RecordReceiver
type StatisticsRecordReceiver struct {
	RecordReceiver
	communication *statistics.Communication
	taskId        int
}

func NewStatisticsRecordReceiver(receiver RecordReceiver, communication *statistics.Communication, taskId int) *StatisticsRecordReceiver {
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
			// 记录接收失败统计
			srr.communication.IncreaseCounter(statistics.WRITE_FAILED_RECORDS, 1)
			if record != nil {
				srr.communication.IncreaseCounter(statistics.WRITE_FAILED_BYTES, int64(record.GetByteSize()))
			}
		}
	} else if record != nil {
		// 记录接收成功统计
		srr.communication.IncreaseCounter(statistics.WRITE_RECEIVED_RECORDS, 1)
		srr.communication.IncreaseCounter(statistics.WRITE_RECEIVED_BYTES, int64(record.GetByteSize()))
	}

	return record, err
}