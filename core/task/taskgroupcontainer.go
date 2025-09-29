package task

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/statistics"
	"github.com/longkeyy/go-datax/common/element"
	coreplugin "github.com/longkeyy/go-datax/core/registry"
	"go.uber.org/zap"
)

// TaskGroupContainer manages the execution of a single reader-writer task pair,
// handling data flow coordination, error propagation, and statistics collection.
type TaskGroupContainer struct {
	configuration config.Configuration
	taskGroupId   int
	communication *statistics.Communication
	communicator  *statistics.TaskCommunicator
}

func NewTaskGroupContainer(configuration config.Configuration, taskGroupId int) *TaskGroupContainer {
	communication := statistics.NewCommunication()
	communicator := statistics.NewTaskCommunicator(configuration, taskGroupId)

	return &TaskGroupContainer{
		configuration: configuration,
		taskGroupId:   taskGroupId,
		communication: communication,
		communicator:  communicator,
	}
}

func (tgc *TaskGroupContainer) Start(readerTaskConfig, writerTaskConfig config.Configuration) error {
	taskLogger := logger.TaskGroupLogger(tgc.taskGroupId)
	taskLogger.Info("TaskGroup starts", zap.Int("taskGroupId", tgc.taskGroupId))

	tgc.communicator.RegisterCommunication(tgc.taskGroupId, tgc.communication)

	startTime := time.Now()

	// Enable cooperative cancellation between reader and writer tasks
	ctx, cancel := context.WithCancel(context.Background())
	// Propagate task group ID for consistent logging across goroutines
	ctx = logger.WithTaskGroupID(ctx, tgc.taskGroupId)
	defer cancel()

	// Optimize buffer size based on data volume hints from split configuration
	bufferSize := tgc.calculateOptimalBufferSize(readerTaskConfig)

	// TODO: Transformer functionality temporarily removed, awaiting future reimplementation
	// transformerExecutions, err := tgc.buildTransformerExecutions()
	// if err != nil {
	//	return fmt.Errorf("failed to build transformer executions: %v", err)
	// }

	// Setup data flow pipeline components
	var recordSender plugin.RecordSender
	var recordReceiver plugin.RecordReceiver

	// TODO: Transformer functionality removed, awaiting future reimplementation
	// if len(transformerExecutions) > 0 {
	//	taskLogger.Warn("Transformers temporarily disabled due to API migration", zap.Int("skippedTransformers", len(transformerExecutions)))
	// }

	taskLogger.Debug("Creating standard channel")
	channel := coreplugin.NewChannel(bufferSize)
	defer channel.Close()
	baseSender := coreplugin.NewRecordSender(channel)
	baseReceiver := coreplugin.NewRecordReceiver(channel)
	recordSender = coreplugin.NewStatisticsRecordSender(baseSender, tgc.communication, tgc.taskGroupId)
	recordReceiver = coreplugin.NewStatisticsRecordReceiver(baseReceiver, tgc.communication, tgc.taskGroupId)

	// Instantiate reader and writer plugins from configuration
	readerTask, err := tgc.createReaderTask(readerTaskConfig)
	if err != nil {
		return fmt.Errorf("failed to create reader task: %v", err)
	}

	writerTask, err := tgc.createWriterTask(writerTaskConfig)
	if err != nil {
		return fmt.Errorf("failed to create writer task: %v", err)
	}

	// Initialize tasks with their specific configurations
	if err := readerTask.Init(readerTaskConfig); err != nil {
		return fmt.Errorf("reader task init failed: %v", err)
	}

	if err := writerTask.Init(writerTaskConfig); err != nil {
		return fmt.Errorf("writer task init failed: %v", err)
	}

	// Prepare phase allows writer to setup tables, validate schema, etc.
	if err := writerTask.Prepare(); err != nil {
		return fmt.Errorf("writer task prepare failed: %v", err)
	}

	// Critical ordering: start writer first to prevent channel backpressure and deadlocks
	var wg sync.WaitGroup
	errChan := make(chan error, 2)
	writerReady := make(chan bool, 1)

	// Start writer (consumer) first to establish ready state
	wg.Add(1)
	go func() {
		defer wg.Done()

		taskLogger.Debug("Writer goroutine started")

		// Signal writer readiness to prevent race conditions
		writerReady <- true
		taskLogger.Debug("Writer ready signal sent")

		taskLogger.Debug("Calling writerTask.StartWrite")

		// Prefer context-aware interface for graceful cancellation support
		if writerWithContext, ok := writerTask.(plugin.WriterTaskWithContext); ok {
			if err := writerWithContext.StartWriteWithContext(recordReceiver, ctx); err != nil {
				if err != context.Canceled {
					taskLogger.Error("Writer task failed", zap.Error(err))
					cancel() // Immediately signal reader to stop on writer failure
					errChan <- fmt.Errorf("writer task failed: %v", err)
				}
			} else {
				taskLogger.Info("Writer task completed successfully")
			}
		} else {
			// Fallback for legacy plugins without context support
			if err := writerTask.StartWrite(recordReceiver); err != nil {
				taskLogger.Error("Writer task failed", zap.Error(err))
				cancel() // Immediately signal reader to stop on writer failure
				errChan <- fmt.Errorf("writer task failed: %v", err)
			} else {
				taskLogger.Info("Writer task completed successfully")
			}
		}
	}()

	// Start reader (producer) only after writer is ready to prevent data loss
	<-writerReady
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer recordSender.Shutdown()

		// Prefer context-aware interface for graceful cancellation support
		if readerWithContext, ok := readerTask.(plugin.ReaderTaskWithContext); ok {
			if err := readerWithContext.StartReadWithContext(recordSender, ctx); err != nil {
				if err != context.Canceled {
					taskLogger.Error("Reader task failed", zap.Error(err))
					cancel() // Signal writer to stop on reader failure
					errChan <- fmt.Errorf("reader task failed: %v", err)
				}
			}
		} else {
			// Fallback for legacy plugins without context support
			if err := readerTask.StartRead(recordSender); err != nil {
				taskLogger.Error("Reader task failed", zap.Error(err))
				cancel() // Signal writer to stop on reader failure
				errChan <- fmt.Errorf("reader task failed: %v", err)
			}
		}
	}()

	// Coordinate task completion with timeout and cancellation handling
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Normal completion path
		close(errChan)

		// Check for task failures
		for err := range errChan {
			return err
		}
	case <-ctx.Done():
		// Context cancelled, ensure clean shutdown
		taskLogger.Warn("Context cancelled, waiting for tasks to cleanup")
		wg.Wait()
		close(errChan)

		// Distinguish between cancellation and actual task errors
		for err := range errChan {
			if err.Error() != "context canceled" {
				return err
			}
		}

		return fmt.Errorf("tasks cancelled due to error in other task")
	}

	// Post-processing phase for cleanup and finalization
	if err := readerTask.Post(); err != nil {
		taskLogger.Warn("Reader post processing failed", zap.Error(err))
	}

	if err := writerTask.Post(); err != nil {
		taskLogger.Warn("Writer post processing failed", zap.Error(err))
	}

	// Resource cleanup phase
	if err := readerTask.Destroy(); err != nil {
		taskLogger.Warn("Reader destroy failed", zap.Error(err))
	}

	if err := writerTask.Destroy(); err != nil {
		taskLogger.Warn("Writer destroy failed", zap.Error(err))
	}

	// TODO: Record transformer statistics - temporarily skipped
	// if transformerChannel != nil {
	//	tgc.logTransformerStatistics(transformerChannel)
	// }

	// Finalize statistics and mark task group as completed
	endTime := time.Now()
	duration := endTime.Sub(startTime)
	tgc.communication.SetState(statistics.StateSucceeded)
	tgc.communication.SetTimestamp(endTime.UnixMilli())

	tgc.communicator.Report(tgc.communication)

	taskLogger.Info("TaskGroup completed",
		zap.Duration("duration", duration),
		zap.Int64("totalRecords", tgc.communication.GetLongCounter(statistics.READ_SUCCEED_RECORDS)),
		zap.Int64("errorRecords", tgc.communication.GetLongCounter(statistics.READ_FAILED_RECORDS)))
	return nil
}

func (tgc *TaskGroupContainer) createReaderTask(readerConfig config.Configuration) (plugin.ReaderTask, error) {
	readerName := readerConfig.GetString("name")
	readerTaskFactory, err := coreplugin.GetReaderTaskFactory(readerName)
	if err != nil {
		return nil, err
	}
	return readerTaskFactory.CreateReaderTask(), nil
}

func (tgc *TaskGroupContainer) createWriterTask(writerConfig config.Configuration) (plugin.WriterTask, error) {
	writerName := writerConfig.GetString("name")
	writerTaskFactory, err := coreplugin.GetWriterTaskFactory(writerName)
	if err != nil {
		return nil, err
	}
	return writerTaskFactory.CreateWriterTask(), nil
}

// calculateOptimalBufferSize determines optimal channel buffer size based on
// split configuration hints to balance memory usage with throughput.
func (tgc *TaskGroupContainer) calculateOptimalBufferSize(readerConfig config.Configuration) int {
	taskLogger := logger.TaskGroupLogger(tgc.taskGroupId)
	// Large dataset indicator: splitRange configuration suggests high-volume task
	if splitRange := readerConfig.Get("parameter.splitRange"); splitRange != nil {
		if rangeMap, ok := splitRange.(map[string]interface{}); ok {
			// Scale buffer size based on expected record count for offset-based splits
			if splitType, exists := rangeMap["type"]; exists && splitType == "offset" {
				if limit, exists := rangeMap["limit"]; exists {
					if limitInt, ok := limit.(int64); ok {
						// Balance memory usage vs throughput: buffer = min(limit/5, 200K), min 10K
						bufferSize := int(limitInt / 5)
						if bufferSize < 10000 {
							bufferSize = 10000
						} else if bufferSize > 200000 {
							bufferSize = 200000
						}
						taskLogger.Info("Dynamic buffer size calculated",
							zap.Int("bufferSize", bufferSize),
							zap.Int64("splitLimit", limitInt),
							zap.String("splitType", "offset"))
						return bufferSize
					}
				}
			}
		}
	}

	// Conservative default for unknown data volumes
	defaultSize := 10000
	taskLogger.Info("Using default buffer size", zap.Int("bufferSize", defaultSize))
	return defaultSize
}

// MockReaderTask provides a test implementation for reader plugins.
// Used for testing task group container logic without external dependencies.
type MockReaderTask struct {
	config config.Configuration
}

func (m *MockReaderTask) Init(config config.Configuration) error {
	m.config = config
	return nil
}

func (m *MockReaderTask) StartRead(recordSender plugin.RecordSender) error {
	// Generate test data for validation purposes
	for i := 0; i < 10; i++ {
		record := element.NewRecord()
		record.AddColumn(element.NewLongColumn(int64(i)))
		record.AddColumn(element.NewStringColumn(fmt.Sprintf("test_data_%d", i)))

		if err := recordSender.SendRecord(record); err != nil {
			return err
		}
	}
	return nil
}

func (m *MockReaderTask) Post() error {
	return nil
}

func (m *MockReaderTask) Destroy() error {
	return nil
}

// MockWriterTask provides a test implementation for writer plugins.
// Used for testing task group container logic without external dependencies.
type MockWriterTask struct {
	config config.Configuration
}

func (m *MockWriterTask) Init(config config.Configuration) error {
	m.config = config
	return nil
}

func (m *MockWriterTask) Prepare() error {
	return nil
}

func (m *MockWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	// Consume test data and log processing activity
	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == coreplugin.ErrChannelClosed {
				break
			}
			return err
		}

		// Use component-level logging for mock implementation tracing
		logger.Component().WithComponent("MockWriter").Debug("Writing record", zap.String("record", record.String()))
	}
	return nil
}

func (m *MockWriterTask) Post() error {
	return nil
}

func (m *MockWriterTask) Destroy() error {
	return nil
}

// TODO: buildTransformerExecutions function removed, awaiting transformer functionality reimplementation
// func (tgc *TaskGroupContainer) buildTransformerExecutions() ([]*transformer.TransformerExecution, error) {
//	// 从job.content[0].transformer读取配置
//	contentList := tgc.configuration.GetListConfiguration("job.content")
//	if len(contentList) == 0 {
//		return nil, nil
//	}
//
//	content := contentList[0]
//	transformerConfigs := content.GetListConfiguration("transformer")
//	if len(transformerConfigs) == 0 {
//		return nil, nil
//	}
//
//	taskLogger := logger.TaskGroupLogger(tgc.taskGroupId)
//	taskLogger.Info("Building transformer executions", zap.Int("count", len(transformerConfigs)))
//
//	executions, err := transformer.BuildTransformerExecutions(transformerConfigs)
//	if err != nil {
//		return nil, err
//	}
//
//	// 记录加载的Transformer
//	for _, execution := range executions {
//		taskLogger.Info("Loaded transformer", zap.String("name", execution.GetTransformerName()))
//	}
//
//	return executions, nil
// }

// logTransformerStatistics records transformer performance metrics - temporarily disabled
// func (tgc *TaskGroupContainer) logTransformerStatistics(transformerChannel *plugin.TransformerChannel) {
//	stats := transformerChannel.GetTransformerStatistics()
//	if len(stats) == 0 {
//		return
//	}
//
//	taskLogger := logger.TaskGroupLogger(tgc.taskGroupId)
//	taskLogger.Info("Transformer Statistics")
//	for name, stat := range stats {
//		taskLogger.Info("Transformer performance",
//			zap.String("name", name),
//			zap.Int64("success", stat["success"]),
//			zap.Int64("failed", stat["failed"]),
//			zap.Int64("filtered", stat["filter"]))
//	}
// }

// logErrorStatistics outputs error statistics for debugging and monitoring.
func (tgc *TaskGroupContainer) logErrorStatistics(errorLimiter *statistics.ErrorLimiter) {
	stats := errorLimiter.GetStatistics()
	taskLogger := logger.TaskGroupLogger(tgc.taskGroupId)
	taskLogger.Info("Job Statistics", zap.String("stats", stats.String()))
}

// GetCommunication returns the communication instance for statistics merging.
func (tgc *TaskGroupContainer) GetCommunication() *statistics.Communication {
	return tgc.communication
}
