package logger

import (
	"time"

	"go.uber.org/zap"
)

// MetricsLogger 性能和指标日志器
type MetricsLogger struct {
	logger ComponentLogger
}

// NewMetricsLogger 创建性能指标日志器
func NewMetricsLogger(component string) *MetricsLogger {
	return &MetricsLogger{
		logger: Component().WithComponent(component),
	}
}

// TaskMetrics 任务执行指标
type TaskMetrics struct {
	StartTime     time.Time
	EndTime       time.Time
	RecordsRead   int64
	RecordsWrite  int64
	BytesRead     int64
	BytesWrite    int64
	ReadThroughput float64 // records/second
	WriteThroughput float64 // records/second
	ErrorCount    int64
}

// Duration 计算任务执行时长
func (m *TaskMetrics) Duration() time.Duration {
	if m.EndTime.IsZero() {
		return time.Since(m.StartTime)
	}
	return m.EndTime.Sub(m.StartTime)
}

// CalculateThroughput 计算吞吐量
func (m *TaskMetrics) CalculateThroughput() {
	duration := m.Duration().Seconds()
	if duration > 0 {
		m.ReadThroughput = float64(m.RecordsRead) / duration
		m.WriteThroughput = float64(m.RecordsWrite) / duration
	}
}

// LogTaskStart 记录任务开始
func (ml *MetricsLogger) LogTaskStart(taskType, taskName string) {
	ml.logger.Info("Task started",
		zap.String("taskType", taskType),
		zap.String("taskName", taskName),
		zap.Time("startTime", time.Now()))
}

// LogTaskComplete 记录任务完成
func (ml *MetricsLogger) LogTaskComplete(taskType, taskName string, metrics *TaskMetrics) {
	metrics.EndTime = time.Now()
	metrics.CalculateThroughput()

	ml.logger.Info("Task completed",
		zap.String("taskType", taskType),
		zap.String("taskName", taskName),
		zap.Duration("duration", metrics.Duration()),
		zap.Int64("recordsRead", metrics.RecordsRead),
		zap.Int64("recordsWrite", metrics.RecordsWrite),
		zap.Float64("readThroughput", metrics.ReadThroughput),
		zap.Float64("writeThroughput", metrics.WriteThroughput),
		zap.Int64("errors", metrics.ErrorCount))
}

// LogTaskError 记录任务错误
func (ml *MetricsLogger) LogTaskError(taskType, taskName string, err error, metrics *TaskMetrics) {
	metrics.ErrorCount++
	ml.logger.Error("Task error",
		zap.String("taskType", taskType),
		zap.String("taskName", taskName),
		zap.Error(err),
		zap.Int64("recordsProcessed", metrics.RecordsRead),
		zap.Int64("totalErrors", metrics.ErrorCount))
}

// LogBatchMetrics 记录批处理指标
func (ml *MetricsLogger) LogBatchMetrics(batchSize, processedRecords int, duration time.Duration, throughput float64) {
	ml.logger.Info("Batch processed",
		zap.Int("batchSize", batchSize),
		zap.Int("processedRecords", processedRecords),
		zap.Duration("processingTime", duration),
		zap.Float64("throughput", throughput))
}

// LogDatabaseMetrics 记录数据库操作指标
func (ml *MetricsLogger) LogDatabaseMetrics(operation string, affectedRows int64, duration time.Duration) {
	ml.logger.Info("Database operation",
		zap.String("operation", operation),
		zap.Int64("affectedRows", affectedRows),
		zap.Duration("duration", duration),
		zap.Float64("rowsPerSecond", float64(affectedRows)/duration.Seconds()))
}

// LogMemoryUsage 记录内存使用情况
func (ml *MetricsLogger) LogMemoryUsage(component string, heapSize, stackSize int64) {
	ml.logger.Debug("Memory usage",
		zap.String("component", component),
		zap.Int64("heapSize", heapSize),
		zap.Int64("stackSize", stackSize))
}

// LogChannelMetrics 记录通道缓冲区指标
func (ml *MetricsLogger) LogChannelMetrics(channelName string, bufferSize, currentSize int, utilization float64) {
	ml.logger.Debug("Channel metrics",
		zap.String("channel", channelName),
		zap.Int("bufferSize", bufferSize),
		zap.Int("currentSize", currentSize),
		zap.Float64("utilization", utilization))
}

// PerformanceTimer 性能计时器
type PerformanceTimer struct {
	startTime time.Time
	logger    *MetricsLogger
	operation string
}

// StartTimer 开始计时
func (ml *MetricsLogger) StartTimer(operation string) *PerformanceTimer {
	return &PerformanceTimer{
		startTime: time.Now(),
		logger:    ml,
		operation: operation,
	}
}

// Stop 停止计时并记录
func (pt *PerformanceTimer) Stop() time.Duration {
	duration := time.Since(pt.startTime)
	pt.logger.logger.Debug("Operation completed",
		zap.String("operation", pt.operation),
		zap.Duration("duration", duration))
	return duration
}

// StopWithCount 停止计时并记录处理数量
func (pt *PerformanceTimer) StopWithCount(count int64) time.Duration {
	duration := time.Since(pt.startTime)
	throughput := float64(count) / duration.Seconds()
	pt.logger.logger.Info("Operation completed with metrics",
		zap.String("operation", pt.operation),
		zap.Duration("duration", duration),
		zap.Int64("count", count),
		zap.Float64("throughput", throughput))
	return duration
}

// 全局便捷方法

// NewTaskMetrics 创建新的任务指标
func NewTaskMetrics() *TaskMetrics {
	return &TaskMetrics{
		StartTime: time.Now(),
	}
}

// Metrics 获取全局指标日志器
func Metrics(component string) *MetricsLogger {
	return NewMetricsLogger(component)
}