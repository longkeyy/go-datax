package plugin

import (
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/statistics"
	"github.com/longkeyy/go-datax/common/transformer"
	"go.uber.org/zap"
)

// TransformerChannel 支持Transformer的通道
type TransformerChannel struct {
	*DefaultChannel
	transformers []*transformer.TransformerExecution
	errorLimiter *statistics.ErrorLimiter
}

func NewTransformerChannel(maxSize int, transformers []*transformer.TransformerExecution) *TransformerChannel {
	return &TransformerChannel{
		DefaultChannel: NewChannel(maxSize),
		transformers:   transformers,
	}
}

func NewTransformerChannelWithErrorLimiter(maxSize int, transformers []*transformer.TransformerExecution, errorLimiter *statistics.ErrorLimiter) *TransformerChannel {
	return &TransformerChannel{
		DefaultChannel: NewChannel(maxSize),
		transformers:   transformers,
		errorLimiter:   errorLimiter,
	}
}

// Push 重写Push方法，在推送数据时应用转换器
func (tc *TransformerChannel) Push(record element.Record) error {
	if tc.closed {
		return ErrChannelClosed
	}

	// 统计总记录数
	if tc.errorLimiter != nil {
		tc.errorLimiter.AddTotalRecord(1)
	}

	// 如果没有转换器，直接推送
	if len(tc.transformers) == 0 {
		return tc.DefaultChannel.Push(record)
	}

	// 应用转换器链
	transformedRecord := record
	var err error

	for _, transformerExecution := range tc.transformers {
		transformedRecord, err = transformerExecution.Execute(transformedRecord)
		if err != nil {
			logger.Component().WithComponent("TransformerChannel").Error("Transformer execution failed",
				zap.String("transformer", transformerExecution.GetTransformerName()),
				zap.Error(err))

			// 统计错误并检查是否超过限制
			if tc.errorLimiter != nil {
				if limitErr := tc.errorLimiter.AddErrorRecord(1); limitErr != nil {
					logger.Component().WithComponent("TransformerChannel").Error("Error limit exceeded", zap.Error(limitErr))
					return limitErr
				}
			}

			return err
		}

		// 如果转换器返回nil，表示记录被过滤掉
		if transformedRecord == nil {
			logger.Component().WithComponent("TransformerChannel").Debug("Record filtered by transformer",
				zap.String("transformer", transformerExecution.GetTransformerName()))

			// 统计过滤记录数
			if tc.errorLimiter != nil {
				tc.errorLimiter.AddFilterRecord(1)
			}

			return nil // 记录被过滤，不推送到通道
		}
	}

	// 推送转换后的记录到底层Channel
	return tc.DefaultChannel.Push(transformedRecord)
}

// TransformerRecordSender 支持Transformer的记录发送器
type TransformerRecordSender struct {
	channel *TransformerChannel
}

func NewTransformerRecordSender(channel *TransformerChannel) *TransformerRecordSender {
	return &TransformerRecordSender{channel: channel}
}

func (s *TransformerRecordSender) SendRecord(record element.Record) error {
	return s.channel.Push(record)
}

func (s *TransformerRecordSender) Flush() error {
	return nil
}

func (s *TransformerRecordSender) Terminate() error {
	return s.channel.Close()
}

func (s *TransformerRecordSender) Shutdown() error {
	return s.channel.Close()
}

// GetTransformerStatistics 获取转换器统计信息
func (tc *TransformerChannel) GetTransformerStatistics() map[string]map[string]int64 {
	stats := make(map[string]map[string]int64)

	for _, transformerExecution := range tc.transformers {
		name := transformerExecution.GetTransformerName()
		stats[name] = map[string]int64{
			"success": transformerExecution.GetSuccessRecords(),
			"failed":  transformerExecution.GetFailedRecords(),
			"filter":  transformerExecution.GetFilterRecords(),
		}
	}

	return stats
}