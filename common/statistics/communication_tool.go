package statistics

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// 统计指标常量定义
const (
	// 基础指标
	STAGE      = "stage"
	BYTE_SPEED = "byteSpeed"
	RECORD_SPEED = "recordSpeed"
	PERCENTAGE = "percentage"

	// 读取相关指标
	READ_SUCCEED_RECORDS = "readSucceedRecords"
	READ_SUCCEED_BYTES   = "readSucceedBytes"
	READ_FAILED_RECORDS  = "readFailedRecords"
	READ_FAILED_BYTES    = "readFailedBytes"

	// 写入相关指标
	WRITE_RECEIVED_RECORDS = "writeReceivedRecords"
	WRITE_RECEIVED_BYTES   = "writeReceivedBytes"
	WRITE_FAILED_RECORDS   = "writeFailedRecords"
	WRITE_FAILED_BYTES     = "writeFailedBytes"

	// 汇总指标
	TOTAL_READ_RECORDS = "totalReadRecords"
	TOTAL_READ_BYTES   = "totalReadBytes"
	TOTAL_ERROR_RECORDS = "totalErrorRecords"
	TOTAL_ERROR_BYTES   = "totalErrorBytes"
	WRITE_SUCCEED_RECORDS = "writeSucceedRecords"
	WRITE_SUCCEED_BYTES   = "writeSucceedBytes"

	// 时间指标
	WAIT_WRITER_TIME = "waitWriterTime"
	WAIT_READER_TIME = "waitReaderTime"

	// Transformer指标
	TRANSFORMER_USED_TIME       = "totalTransformerUsedTime"
	TRANSFORMER_SUCCEED_RECORDS = "totalTransformerSuccessRecords"
	TRANSFORMER_FAILED_RECORDS  = "totalTransformerFailedRecords"
	TRANSFORMER_FILTER_RECORDS  = "totalTransformerFilterRecords"
	TRANSFORMER_NAME_PREFIX     = "usedTimeByTransformer_"
)

// CommunicationTool 提供Communication的业务层处理工具
type CommunicationTool struct{}

// GetReportCommunication 为汇报准备Communication数据
func (ct *CommunicationTool) GetReportCommunication(now, old *Communication, totalStage int) *Communication {
	if now == nil || old == nil {
		panic("为汇报准备的新旧metric不能为nil")
	}

	totalReadRecords := ct.GetTotalReadRecords(now)
	totalReadBytes := ct.GetTotalReadBytes(now)

	now.SetLongCounter(TOTAL_READ_RECORDS, totalReadRecords)
	now.SetLongCounter(TOTAL_READ_BYTES, totalReadBytes)
	now.SetLongCounter(TOTAL_ERROR_RECORDS, ct.GetTotalErrorRecords(now))
	now.SetLongCounter(TOTAL_ERROR_BYTES, ct.GetTotalErrorBytes(now))
	now.SetLongCounter(WRITE_SUCCEED_RECORDS, ct.GetWriteSucceedRecords(now))
	now.SetLongCounter(WRITE_SUCCEED_BYTES, ct.GetWriteSucceedBytes(now))

	timeInterval := now.GetTimestamp() - old.GetTimestamp()
	var sec int64 = 1
	if timeInterval > 1000 {
		sec = timeInterval / 1000
	}

	bytesSpeed := (totalReadBytes - ct.GetTotalReadBytes(old)) / sec
	recordsSpeed := (totalReadRecords - ct.GetTotalReadRecords(old)) / sec

	if bytesSpeed < 0 {
		bytesSpeed = 0
	}
	if recordsSpeed < 0 {
		recordsSpeed = 0
	}

	now.SetLongCounter(BYTE_SPEED, bytesSpeed)
	now.SetLongCounter(RECORD_SPEED, recordsSpeed)

	// 计算百分比
	if totalStage > 0 {
		percentage := float64(now.GetLongCounter(STAGE)) / float64(totalStage)
		now.SetLongCounter(PERCENTAGE, int64(percentage*100))
	}

	if old.GetThrowable() != nil {
		now.SetThrowable(old.GetThrowable())
	}

	return now
}

// GetTotalReadRecords 获取总读取记录数
func (ct *CommunicationTool) GetTotalReadRecords(communication *Communication) int64 {
	return communication.GetLongCounter(READ_SUCCEED_RECORDS) +
		communication.GetLongCounter(READ_FAILED_RECORDS)
}

// GetTotalReadBytes 获取总读取字节数
func (ct *CommunicationTool) GetTotalReadBytes(communication *Communication) int64 {
	return communication.GetLongCounter(READ_SUCCEED_BYTES) +
		communication.GetLongCounter(READ_FAILED_BYTES)
}

// GetTotalErrorRecords 获取总错误记录数
func (ct *CommunicationTool) GetTotalErrorRecords(communication *Communication) int64 {
	return communication.GetLongCounter(READ_FAILED_RECORDS) +
		communication.GetLongCounter(WRITE_FAILED_RECORDS)
}

// GetTotalErrorBytes 获取总错误字节数
func (ct *CommunicationTool) GetTotalErrorBytes(communication *Communication) int64 {
	return communication.GetLongCounter(READ_FAILED_BYTES) +
		communication.GetLongCounter(WRITE_FAILED_BYTES)
}

// GetWriteSucceedRecords 获取写入成功记录数
func (ct *CommunicationTool) GetWriteSucceedRecords(communication *Communication) int64 {
	return communication.GetLongCounter(WRITE_RECEIVED_RECORDS) -
		communication.GetLongCounter(WRITE_FAILED_RECORDS)
}

// GetWriteSucceedBytes 获取写入成功字节数
func (ct *CommunicationTool) GetWriteSucceedBytes(communication *Communication) int64 {
	return communication.GetLongCounter(WRITE_RECEIVED_BYTES) -
		communication.GetLongCounter(WRITE_FAILED_BYTES)
}

// ProgressSnapshot 进度快照结构
type ProgressSnapshot struct {
	Total      string
	Speed      string
	Error      string
	WaitWriter string
	WaitReader string
	Transformer string
	Percentage string
}

// GetSnapshot 获取进度快照字符串
func (ct *CommunicationTool) GetSnapshot(communication *Communication) string {
	snapshot := ct.GetSnapshotStruct(communication)

	var sb strings.Builder
	sb.WriteString("Total ")
	sb.WriteString(snapshot.Total)
	sb.WriteString(" | Speed ")
	sb.WriteString(snapshot.Speed)
	sb.WriteString(" | Error ")
	sb.WriteString(snapshot.Error)
	sb.WriteString(" | All Task WaitWriterTime ")
	sb.WriteString(snapshot.WaitWriter)
	sb.WriteString(" | All Task WaitReaderTime ")
	sb.WriteString(snapshot.WaitReader)
	sb.WriteString(" | ")

	if snapshot.Transformer != "" {
		sb.WriteString(snapshot.Transformer)
		sb.WriteString(" | ")
	}

	sb.WriteString("Percentage ")
	sb.WriteString(snapshot.Percentage)

	return sb.String()
}

// GetSnapshotStruct 获取结构化的进度快照
func (ct *CommunicationTool) GetSnapshotStruct(communication *Communication) ProgressSnapshot {
	total := ct.getTotal(communication)
	speed := ct.getSpeed(communication)
	errorInfo := ct.getError(communication)
	waitWriter := ct.FormatTime(communication.GetLongCounter(WAIT_WRITER_TIME))
	waitReader := ct.FormatTime(communication.GetLongCounter(WAIT_READER_TIME))
	transformer := ct.getTransformer(communication)
	percentage := ct.getPercentage(communication)

	return ProgressSnapshot{
		Total:       total,
		Speed:       speed,
		Error:       errorInfo,
		WaitWriter:  waitWriter,
		WaitReader:  waitReader,
		Transformer: transformer,
		Percentage:  percentage,
	}
}

func (ct *CommunicationTool) getTotal(communication *Communication) string {
	return fmt.Sprintf("%d records, %s",
		communication.GetLongCounter(TOTAL_READ_RECORDS),
		ct.FormatBytes(communication.GetLongCounter(TOTAL_READ_BYTES)))
}

func (ct *CommunicationTool) getSpeed(communication *Communication) string {
	return fmt.Sprintf("%s/s, %d records/s",
		ct.FormatBytes(communication.GetLongCounter(BYTE_SPEED)),
		communication.GetLongCounter(RECORD_SPEED))
}

func (ct *CommunicationTool) getError(communication *Communication) string {
	return fmt.Sprintf("%d records, %s",
		communication.GetLongCounter(TOTAL_ERROR_RECORDS),
		ct.FormatBytes(communication.GetLongCounter(TOTAL_ERROR_BYTES)))
}

func (ct *CommunicationTool) getTransformer(communication *Communication) string {
	if communication.GetLongCounter(TRANSFORMER_USED_TIME) > 0 ||
		communication.GetLongCounter(TRANSFORMER_SUCCEED_RECORDS) > 0 ||
		communication.GetLongCounter(TRANSFORMER_FAILED_RECORDS) > 0 ||
		communication.GetLongCounter(TRANSFORMER_FILTER_RECORDS) > 0 {

		return fmt.Sprintf("Transformer Success %d records | Transformer Error %d records | Transformer Filter %d records | Transformer usedTime %s",
			communication.GetLongCounter(TRANSFORMER_SUCCEED_RECORDS),
			communication.GetLongCounter(TRANSFORMER_FAILED_RECORDS),
			communication.GetLongCounter(TRANSFORMER_FILTER_RECORDS),
			ct.FormatTime(communication.GetLongCounter(TRANSFORMER_USED_TIME)))
	}
	return ""
}

func (ct *CommunicationTool) getPercentage(communication *Communication) string {
	percentage := communication.GetLongCounter(PERCENTAGE)
	return fmt.Sprintf("%.2f%%", float64(percentage))
}

// formatBytes 格式化字节数显示
func (ct *CommunicationTool) FormatBytes(bytes int64) string {
	if bytes < 0 {
		return "0B"
	}

	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	if exp >= len(units) {
		exp = len(units) - 1
	}

	return fmt.Sprintf("%.2f%s", float64(bytes)/float64(div), units[exp+1])
}

// formatTime 格式化时间显示（毫秒转为易读格式）
func (ct *CommunicationTool) FormatTime(millis int64) string {
	if millis < 0 {
		return "0ms"
	}

	duration := time.Duration(millis) * time.Millisecond

	if duration < time.Second {
		return fmt.Sprintf("%dms", millis)
	} else if duration < time.Minute {
		return fmt.Sprintf("%.2fs", duration.Seconds())
	} else if duration < time.Hour {
		return fmt.Sprintf("%.2fm", duration.Minutes())
	} else {
		return fmt.Sprintf("%.2fh", duration.Hours())
	}
}

// JSONSnapshot JSON格式的进度快照
type JSONSnapshot struct {
	TotalBytes    int64   `json:"totalBytes"`
	TotalRecords  int64   `json:"totalRecords"`
	SpeedBytes    int64   `json:"speedBytes"`
	SpeedRecords  int64   `json:"speedRecords"`
	Stage         int64   `json:"stage"`
	ErrorRecords  int64   `json:"errorRecords"`
	ErrorBytes    int64   `json:"errorBytes"`
	ErrorMessage  string  `json:"errorMessage"`
	Percentage    float64 `json:"percentage"`
	WaitReaderTime int64  `json:"waitReaderTime"`
	WaitWriterTime int64  `json:"waitWriterTime"`
}

// GetJSONSnapshot 获取JSON格式的进度快照
func (ct *CommunicationTool) GetJSONSnapshot(communication *Communication) (string, error) {
	snapshot := JSONSnapshot{
		TotalBytes:    communication.GetLongCounter(TOTAL_READ_BYTES),
		TotalRecords:  communication.GetLongCounter(TOTAL_READ_RECORDS),
		SpeedBytes:    communication.GetLongCounter(BYTE_SPEED),
		SpeedRecords:  communication.GetLongCounter(RECORD_SPEED),
		Stage:         communication.GetLongCounter(STAGE),
		ErrorRecords:  communication.GetLongCounter(TOTAL_ERROR_RECORDS),
		ErrorBytes:    communication.GetLongCounter(TOTAL_ERROR_BYTES),
		ErrorMessage:  communication.GetThrowableMessage(),
		Percentage:    float64(communication.GetLongCounter(PERCENTAGE)),
		WaitReaderTime: communication.GetLongCounter(WAIT_READER_TIME),
		WaitWriterTime: communication.GetLongCounter(WAIT_WRITER_TIME),
	}

	bytes, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// 全局CommunicationTool实例
var DefaultCommunicationTool = &CommunicationTool{}