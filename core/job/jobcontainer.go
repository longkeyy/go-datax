package job

import (
	"fmt"
	"sync"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/core/taskgroup"
	"go.uber.org/zap"
)

// JobContainer 作业容器，负责作业的生命周期管理
type JobContainer struct {
	configuration *config.Configuration
	readerPlugin  string
	writerPlugin  string
	needChannels  int
}

func NewJobContainer(configuration *config.Configuration) *JobContainer {
	return &JobContainer{
		configuration: configuration,
	}
}

func (jc *JobContainer) Start() error {
	appLogger := logger.App()
	appLogger.Info("DataX JobContainer starts job")

	startTime := time.Now()
	var err error

	defer func() {
		endTime := time.Now()
		duration := endTime.Sub(startTime)
		appLogger.Info("Job completed", zap.Duration("duration", duration))
	}()

	// 初始化
	if err = jc.init(); err != nil {
		return fmt.Errorf("job initialization failed: %v", err)
	}

	// 准备阶段
	if err = jc.prepare(); err != nil {
		return fmt.Errorf("job preparation failed: %v", err)
	}

	// 拆分任务
	readerTaskConfigs, writerTaskConfigs, err := jc.split()
	if err != nil {
		return fmt.Errorf("job split failed: %v", err)
	}

	// 调度执行
	if err = jc.schedule(readerTaskConfigs, writerTaskConfigs); err != nil {
		return fmt.Errorf("job schedule failed: %v", err)
	}

	// 后处理
	if err = jc.post(); err != nil {
		return fmt.Errorf("job post processing failed: %v", err)
	}

	return nil
}

func (jc *JobContainer) init() error {
	// 从配置中获取插件信息
	contentList := jc.configuration.GetListConfiguration("job.content")
	if len(contentList) == 0 {
		return fmt.Errorf("job.content is empty")
	}

	// 目前只支持单个content
	content := contentList[0]
	jc.readerPlugin = content.GetString("reader.name")
	jc.writerPlugin = content.GetString("writer.name")

	if jc.readerPlugin == "" {
		return fmt.Errorf("reader plugin name is required")
	}
	if jc.writerPlugin == "" {
		return fmt.Errorf("writer plugin name is required")
	}

	// 设置通道数
	jc.needChannels = jc.configuration.GetIntWithDefault("job.setting.speed.channel", 1)
	if jc.needChannels <= 0 {
		jc.needChannels = 1
	}

	appLogger := logger.App()
	appLogger.Info("Job configuration initialized",
		zap.String("readerPlugin", jc.readerPlugin),
		zap.String("writerPlugin", jc.writerPlugin),
		zap.Int("channels", jc.needChannels))

	return nil
}

func (jc *JobContainer) prepare() error {
	// 执行Writer Job的prepare操作
	contentList := jc.configuration.GetListConfiguration("job.content")
	content := contentList[0]

	// 创建Writer Job并执行prepare
	writerJob, err := jc.createWriterJob(content.GetConfiguration("writer"))
	if err != nil {
		return fmt.Errorf("failed to create writer job for prepare: %v", err)
	}

	if err := writerJob.Prepare(); err != nil {
		return fmt.Errorf("writer job prepare failed: %v", err)
	}

	return nil
}

func (jc *JobContainer) split() ([]*config.Configuration, []*config.Configuration, error) {
	contentList := jc.configuration.GetListConfiguration("job.content")
	content := contentList[0]

	// 创建Reader Job实例进行split
	readerJob, err := jc.createReaderJob(content.GetConfiguration("reader"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create reader job: %v", err)
	}

	readerTaskConfigs, err := readerJob.Split(jc.needChannels)
	if err != nil {
		return nil, nil, fmt.Errorf("reader split failed: %v", err)
	}

	// 创建Writer Job实例进行split
	writerJob, err := jc.createWriterJob(content.GetConfiguration("writer"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create writer job: %v", err)
	}

	writerTaskConfigs, err := writerJob.Split(len(readerTaskConfigs))
	if err != nil {
		return nil, nil, fmt.Errorf("writer split failed: %v", err)
	}

	appLogger := logger.App()
	appLogger.Info("Task splitting completed",
		zap.Int("readerTasks", len(readerTaskConfigs)),
		zap.Int("writerTasks", len(writerTaskConfigs)))

	return readerTaskConfigs, writerTaskConfigs, nil
}

func (jc *JobContainer) schedule(readerTaskConfigs, writerTaskConfigs []*config.Configuration) error {
	if len(readerTaskConfigs) != len(writerTaskConfigs) {
		return fmt.Errorf("reader tasks (%d) and writer tasks (%d) count mismatch",
			len(readerTaskConfigs), len(writerTaskConfigs))
	}

	taskCount := len(readerTaskConfigs)
	appLogger := logger.App()
	appLogger.Info("Starting task groups", zap.Int("taskGroups", taskCount))

	// 创建TaskGroup并发执行
	var wg sync.WaitGroup
	errChan := make(chan error, taskCount)

	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			taskGroupConfig := jc.configuration.Clone()
			taskGroupConfig.Set("taskGroup.id", index)
			taskGroupConfig.Set("taskGroup.reader", readerTaskConfigs[index].Get(""))
			taskGroupConfig.Set("taskGroup.writer", writerTaskConfigs[index].Get(""))

			taskGroupContainer := taskgroup.NewTaskGroupContainer(taskGroupConfig, index)
			if err := taskGroupContainer.Start(readerTaskConfigs[index], writerTaskConfigs[index]); err != nil {
				errChan <- fmt.Errorf("taskGroup %d failed: %v", index, err)
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	// 检查是否有错误
	for err := range errChan {
		return err
	}

	return nil
}

func (jc *JobContainer) post() error {
	// 后处理逻辑
	return nil
}

func (jc *JobContainer) createReaderJob(readerConfig *config.Configuration) (plugin.ReaderJob, error) {
	readerName := readerConfig.GetString("name")
	readerJob, err := plugin.CreateReaderJob(readerName)
	if err != nil {
		return nil, err
	}

	if err := readerJob.Init(readerConfig); err != nil {
		return nil, fmt.Errorf("failed to init reader job: %v", err)
	}

	return readerJob, nil
}

func (jc *JobContainer) createWriterJob(writerConfig *config.Configuration) (plugin.WriterJob, error) {
	writerName := writerConfig.GetString("name")
	writerJob, err := plugin.CreateWriterJob(writerName)
	if err != nil {
		return nil, err
	}

	if err := writerJob.Init(writerConfig); err != nil {
		return nil, fmt.Errorf("failed to init writer job: %v", err)
	}

	return writerJob, nil
}

// MockReaderJob 模拟Reader Job实现
type MockReaderJob struct {
	config *config.Configuration
}

func (m *MockReaderJob) Init(config *config.Configuration) error {
	m.config = config
	return nil
}

func (m *MockReaderJob) Split(adviceNumber int) ([]*config.Configuration, error) {
	// 简单拆分：每个任务使用相同配置
	configs := make([]*config.Configuration, adviceNumber)
	for i := 0; i < adviceNumber; i++ {
		configs[i] = m.config.Clone()
		configs[i].Set("taskId", i)
	}
	return configs, nil
}

func (m *MockReaderJob) Post() error {
	return nil
}

func (m *MockReaderJob) Destroy() error {
	return nil
}

// MockWriterJob 模拟Writer Job实现
type MockWriterJob struct {
	config *config.Configuration
}

func (m *MockWriterJob) Init(config *config.Configuration) error {
	m.config = config
	return nil
}

func (m *MockWriterJob) Split(mandatoryNumber int) ([]*config.Configuration, error) {
	// 简单拆分：每个任务使用相同配置
	configs := make([]*config.Configuration, mandatoryNumber)
	for i := 0; i < mandatoryNumber; i++ {
		configs[i] = m.config.Clone()
		configs[i].Set("taskId", i)
	}
	return configs, nil
}

func (m *MockWriterJob) Post() error {
	return nil
}

func (m *MockWriterJob) Prepare() error {
	return nil
}

func (m *MockWriterJob) Destroy() error {
	return nil
}