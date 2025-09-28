package taskgroup

import (
	"context"
	"fmt"
	"sync"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/statistics"
	"github.com/longkeyy/go-datax/common/transformer"
	"go.uber.org/zap"
)

// TaskGroupContainer 任务组容器
type TaskGroupContainer struct {
	configuration *config.Configuration
	taskGroupId   int
}

func NewTaskGroupContainer(configuration *config.Configuration, taskGroupId int) *TaskGroupContainer {
	return &TaskGroupContainer{
		configuration: configuration,
		taskGroupId:   taskGroupId,
	}
}

func (tgc *TaskGroupContainer) Start(readerTaskConfig, writerTaskConfig *config.Configuration) error {
	// 创建带任务组上下文的日志器
	taskLogger := logger.TaskGroupLogger(tgc.taskGroupId)
	taskLogger.Info("TaskGroup starts", zap.Int("taskGroupId", tgc.taskGroupId))

	// 创建可取消的context，用于任务间的协调取消
	ctx, cancel := context.WithCancel(context.Background())
	// 添加任务组信息到context中，用于子任务日志
	ctx = logger.WithTaskGroupID(ctx, tgc.taskGroupId)
	defer cancel()

	// 动态计算缓冲区大小：基于任务预期数据量
	bufferSize := tgc.calculateOptimalBufferSize(readerTaskConfig)

	// 构建Transformer执行器
	transformerExecutions, err := tgc.buildTransformerExecutions()
	if err != nil {
		return fmt.Errorf("failed to build transformer executions: %v", err)
	}

	// 创建通道和RecordSender/RecordReceiver
	var recordSender plugin.RecordSender
	var recordReceiver plugin.RecordReceiver
	var transformerChannel *plugin.TransformerChannel

	if len(transformerExecutions) > 0 {
		// 如果有Transformer，使用TransformerChannel
		taskLogger.Info("Creating TransformerChannel", zap.Int("transformerCount", len(transformerExecutions)))
		transformerChannel = plugin.NewTransformerChannel(bufferSize, transformerExecutions)
		defer transformerChannel.Close()
		recordSender = plugin.NewTransformerRecordSender(transformerChannel)
		recordReceiver = plugin.NewRecordReceiver(transformerChannel.DefaultChannel)
	} else {
		// 如果没有Transformer，使用普通Channel
		taskLogger.Debug("Creating standard channel (no transformers)")
		channel := plugin.NewChannel(bufferSize)
		defer channel.Close()
		recordSender = plugin.NewRecordSender(channel)
		recordReceiver = plugin.NewRecordReceiver(channel)
	}

	// 创建Reader和Writer任务
	readerTask, err := tgc.createReaderTask(readerTaskConfig)
	if err != nil {
		return fmt.Errorf("failed to create reader task: %v", err)
	}

	writerTask, err := tgc.createWriterTask(writerTaskConfig)
	if err != nil {
		return fmt.Errorf("failed to create writer task: %v", err)
	}

	// 初始化任务
	if err := readerTask.Init(readerTaskConfig); err != nil {
		return fmt.Errorf("reader task init failed: %v", err)
	}

	if err := writerTask.Init(writerTaskConfig); err != nil {
		return fmt.Errorf("writer task init failed: %v", err)
	}

	// 准备阶段
	if err := writerTask.Prepare(); err != nil {
		return fmt.Errorf("writer task prepare failed: %v", err)
	}

	// 启动Reader和Writer（重要：先启动Writer再启动Reader，避免channel积压）
	var wg sync.WaitGroup
	errChan := make(chan error, 2)
	writerReady := make(chan bool, 1)

	// 先启动Writer（消费者）
	wg.Add(1)
	go func() {
		defer wg.Done()

		taskLogger.Debug("Writer goroutine started")

		// 通知Writer已准备就绪
		writerReady <- true
		taskLogger.Debug("Writer ready signal sent")

		taskLogger.Debug("Calling writerTask.StartWrite")

		// 使用带Context的StartWrite接口（如果支持的话）
		if writerWithContext, ok := writerTask.(plugin.WriterTaskWithContext); ok {
			if err := writerWithContext.StartWriteWithContext(recordReceiver, ctx); err != nil {
				if err != context.Canceled {
					taskLogger.Error("Writer task failed", zap.Error(err))
					cancel() // 立即通知Reader停止
					errChan <- fmt.Errorf("writer task failed: %v", err)
				}
			} else {
				taskLogger.Info("Writer task completed successfully")
			}
		} else {
			// 回退到原始接口
			if err := writerTask.StartWrite(recordReceiver); err != nil {
				taskLogger.Error("Writer task failed", zap.Error(err))
				cancel() // 立即通知Reader停止
				errChan <- fmt.Errorf("writer task failed: %v", err)
			} else {
				taskLogger.Info("Writer task completed successfully")
			}
		}
	}()

	// 等待Writer准备就绪后再启动Reader（生产者）
	<-writerReady
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer recordSender.Shutdown()

		// 使用带Context的StartRead接口（如果支持的话）
		if readerWithContext, ok := readerTask.(plugin.ReaderTaskWithContext); ok {
			if err := readerWithContext.StartReadWithContext(recordSender, ctx); err != nil {
				if err != context.Canceled {
					taskLogger.Error("Reader task failed", zap.Error(err))
					cancel() // 通知Writer停止
					errChan <- fmt.Errorf("reader task failed: %v", err)
				}
			}
		} else {
			// 回退到原始接口
			if err := readerTask.StartRead(recordSender); err != nil {
				taskLogger.Error("Reader task failed", zap.Error(err))
				cancel() // 通知Writer停止
				errChan <- fmt.Errorf("reader task failed: %v", err)
			}
		}
	}()

	// 等待任务完成或Context取消
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 正常完成
		close(errChan)

		// 检查错误
		for err := range errChan {
			return err
		}
	case <-ctx.Done():
		// Context取消，等待goroutine退出
		taskLogger.Warn("Context cancelled, waiting for tasks to cleanup")
		wg.Wait()
		close(errChan)

		// 检查是否有真实错误（非取消）
		for err := range errChan {
			if err.Error() != "context canceled" {
				return err
			}
		}

		return fmt.Errorf("tasks cancelled due to error in other task")
	}

	// 后处理
	if err := readerTask.Post(); err != nil {
		taskLogger.Warn("Reader post processing failed", zap.Error(err))
	}

	if err := writerTask.Post(); err != nil {
		taskLogger.Warn("Writer post processing failed", zap.Error(err))
	}

	// 销毁资源
	if err := readerTask.Destroy(); err != nil {
		taskLogger.Warn("Reader destroy failed", zap.Error(err))
	}

	if err := writerTask.Destroy(); err != nil {
		taskLogger.Warn("Writer destroy failed", zap.Error(err))
	}

	// 记录Transformer统计信息
	if transformerChannel != nil {
		tgc.logTransformerStatistics(transformerChannel)
	}

	taskLogger.Info("TaskGroup completed")
	return nil
}

func (tgc *TaskGroupContainer) createReaderTask(readerConfig *config.Configuration) (plugin.ReaderTask, error) {
	readerName := readerConfig.GetString("name")
	return plugin.CreateReaderTask(readerName)
}

func (tgc *TaskGroupContainer) createWriterTask(writerConfig *config.Configuration) (plugin.WriterTask, error) {
	writerName := writerConfig.GetString("name")
	return plugin.CreateWriterTask(writerName)
}

// calculateOptimalBufferSize 计算最优的通道缓冲区大小
func (tgc *TaskGroupContainer) calculateOptimalBufferSize(readerConfig *config.Configuration) int {
	taskLogger := logger.TaskGroupLogger(tgc.taskGroupId)
	// 检查是否有splitRange配置，如果有，说明是大数据集的分片任务
	if splitRange := readerConfig.Get("parameter.splitRange"); splitRange != nil {
		if rangeMap, ok := splitRange.(map[string]interface{}); ok {
			// 如果是offset类型的分片，根据limit值动态调整缓冲区大小
			if splitType, exists := rangeMap["type"]; exists && splitType == "offset" {
				if limit, exists := rangeMap["limit"]; exists {
					if limitInt, ok := limit.(int64); ok {
						// 对于大数据集，使用更大的缓冲区但有上限
						// 基本策略：缓冲区大小 = min(limit/5, 200000)，但至少10000
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

	// 默认缓冲区大小
	defaultSize := 10000
	taskLogger.Info("Using default buffer size", zap.Int("bufferSize", defaultSize))
	return defaultSize
}

// MockReaderTask 模拟Reader Task实现
type MockReaderTask struct {
	config *config.Configuration
}

func (m *MockReaderTask) Init(config *config.Configuration) error {
	m.config = config
	return nil
}

func (m *MockReaderTask) StartRead(recordSender plugin.RecordSender) error {
	// 模拟读取数据并发送
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

// MockWriterTask 模拟Writer Task实现
type MockWriterTask struct {
	config *config.Configuration
}

func (m *MockWriterTask) Init(config *config.Configuration) error {
	m.config = config
	return nil
}

func (m *MockWriterTask) Prepare() error {
	return nil
}

func (m *MockWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	// 模拟接收数据并写入
	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == plugin.ErrChannelClosed {
				break
			}
			return err
		}

		// 使用component级别日志，因为这是Mock组件
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

// buildTransformerExecutions 从全局配置构建Transformer执行器
func (tgc *TaskGroupContainer) buildTransformerExecutions() ([]*transformer.TransformerExecution, error) {
	// 从job.content[0].transformer读取配置
	contentList := tgc.configuration.GetListConfiguration("job.content")
	if len(contentList) == 0 {
		return nil, nil
	}

	content := contentList[0]
	transformerConfigs := content.GetListConfiguration("transformer")
	if len(transformerConfigs) == 0 {
		return nil, nil
	}

	taskLogger := logger.TaskGroupLogger(tgc.taskGroupId)
	taskLogger.Info("Building transformer executions", zap.Int("count", len(transformerConfigs)))

	executions, err := transformer.BuildTransformerExecutions(transformerConfigs)
	if err != nil {
		return nil, err
	}

	// 记录加载的Transformer
	for _, execution := range executions {
		taskLogger.Info("Loaded transformer", zap.String("name", execution.GetTransformerName()))
	}

	return executions, nil
}

// logTransformerStatistics 记录Transformer统计信息
func (tgc *TaskGroupContainer) logTransformerStatistics(transformerChannel *plugin.TransformerChannel) {
	stats := transformerChannel.GetTransformerStatistics()
	if len(stats) == 0 {
		return
	}

	taskLogger := logger.TaskGroupLogger(tgc.taskGroupId)
	taskLogger.Info("Transformer Statistics")
	for name, stat := range stats {
		taskLogger.Info("Transformer performance",
			zap.String("name", name),
			zap.Int64("success", stat["success"]),
			zap.Int64("failed", stat["failed"]),
			zap.Int64("filtered", stat["filter"]))
	}
}

// logErrorStatistics 记录错误统计信息
func (tgc *TaskGroupContainer) logErrorStatistics(errorLimiter *statistics.ErrorLimiter) {
	stats := errorLimiter.GetStatistics()
	taskLogger := logger.TaskGroupLogger(tgc.taskGroupId)
	taskLogger.Info("Job Statistics", zap.String("stats", stats.String()))
}
