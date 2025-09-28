package taskgroup

import (
	"fmt"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/statistics"
	"github.com/longkeyy/go-datax/common/transformer"
	"log"
	"sync"
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
	log.Printf("TaskGroup %d starts", tgc.taskGroupId)

	// 暂时简化实现：使用普通Channel，Transformer功能在Writer中处理
	// 动态计算缓冲区大小：基于任务预期数据量
	bufferSize := tgc.calculateOptimalBufferSize(readerTaskConfig)
	channel := plugin.NewChannel(bufferSize)
	defer channel.Close()

	// 创建RecordSender和RecordReceiver
	recordSender := plugin.NewRecordSender(channel)
	recordReceiver := plugin.NewRecordReceiver(channel)

	// TODO: 重新实现Transformer功能
	// transformerExecutions, err := tgc.buildTransformerExecutions()
	// if err != nil {
	//     return fmt.Errorf("failed to build transformer executions: %v", err)
	// }

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

	// 启动Reader和Writer
	var wg sync.WaitGroup
	errChan := make(chan error, 2)

	// 启动Reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer recordSender.Shutdown()

		if err := readerTask.StartRead(recordSender); err != nil {
			errChan <- fmt.Errorf("reader task failed: %v", err)
		}
	}()

	// 启动Writer
	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := writerTask.StartWrite(recordReceiver); err != nil {
			errChan <- fmt.Errorf("writer task failed: %v", err)
		}
	}()

	wg.Wait()
	close(errChan)

	// 检查错误
	for err := range errChan {
		return err
	}

	// 后处理
	if err := readerTask.Post(); err != nil {
		log.Printf("Reader post processing failed: %v", err)
	}

	if err := writerTask.Post(); err != nil {
		log.Printf("Writer post processing failed: %v", err)
	}

	// 销毁资源
	if err := readerTask.Destroy(); err != nil {
		log.Printf("Reader destroy failed: %v", err)
	}

	if err := writerTask.Destroy(); err != nil {
		log.Printf("Writer destroy failed: %v", err)
	}

	log.Printf("TaskGroup %d completed", tgc.taskGroupId)
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
						log.Printf("TaskGroup %d: Dynamic buffer size %d for offset split (limit=%d)",
							tgc.taskGroupId, bufferSize, limitInt)
						return bufferSize
					}
				}
			}
		}
	}

	// 默认缓冲区大小
	defaultSize := 10000
	log.Printf("TaskGroup %d: Using default buffer size %d", tgc.taskGroupId, defaultSize)
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

		log.Printf("Writing record: %s", record.String())
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

	log.Printf("Building %d transformer executions", len(transformerConfigs))

	executions, err := transformer.BuildTransformerExecutions(transformerConfigs)
	if err != nil {
		return nil, err
	}

	// 记录加载的Transformer
	for _, execution := range executions {
		log.Printf("Loaded transformer: %s", execution.GetTransformerName())
	}

	return executions, nil
}

// logTransformerStatistics 记录Transformer统计信息
func (tgc *TaskGroupContainer) logTransformerStatistics(transformerChannel *plugin.TransformerChannel) {
	stats := transformerChannel.GetTransformerStatistics()
	if len(stats) == 0 {
		return
	}

	log.Printf("TaskGroup %d Transformer Statistics:", tgc.taskGroupId)
	for name, stat := range stats {
		log.Printf("  %s: success=%d, failed=%d, filtered=%d",
			name, stat["success"], stat["failed"], stat["filter"])
	}
}

// logErrorStatistics 记录错误统计信息
func (tgc *TaskGroupContainer) logErrorStatistics(errorLimiter *statistics.ErrorLimiter) {
	stats := errorLimiter.GetStatistics()
	log.Printf("TaskGroup %d Job Statistics: %s", tgc.taskGroupId, stats.String())
}

