package job

import (
	"fmt"
	"sync"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/statistics"
	"github.com/longkeyy/go-datax/core/taskgroup"
	"go.uber.org/zap"
)

// JobContainer 作业容器，负责作业的生命周期管理
type JobContainer struct {
	configuration   *config.Configuration
	readerPlugin    string
	writerPlugin    string
	needChannels    int
	communicator    *statistics.JobContainerCommunicator
	schedulerReporter *statistics.SchedulerReporter
	totalStage      int
	startTimeStamp  int64
	endTimeStamp    int64
	startTransferTimeStamp int64
	endTransferTimeStamp   int64
}

func NewJobContainer(configuration *config.Configuration) *JobContainer {
	communicator := statistics.NewJobContainerCommunicator(configuration)
	return &JobContainer{
		configuration: configuration,
		communicator:  communicator,
	}
}

func (jc *JobContainer) Start() error {
	appLogger := logger.App()
	appLogger.Info("DataX JobContainer starts job")

	// 记录开始时间
	startTime := time.Now()
	jc.startTimeStamp = startTime.UnixMilli()
	var err error
	hasException := false

	defer func() {
		// 停止调度器汇报
		if jc.schedulerReporter != nil {
			jc.schedulerReporter.Stop()
		}

		endTime := time.Now()
		jc.endTimeStamp = endTime.UnixMilli()
		duration := endTime.Sub(startTime)

		if !hasException {
			// 输出最终统计信息
			jc.logStatistics()
		}

		appLogger.Info("Job completed", zap.Duration("duration", duration))
	}()

	defer func() {
		if r := recover(); r != nil {
			hasException = true
			appLogger.Error("Job panicked", zap.Any("panic", r))
		}
	}()

	// 初始化
	if err = jc.init(); err != nil {
		hasException = true
		return fmt.Errorf("job initialization failed: %v", err)
	}

	// 准备阶段
	if err = jc.prepare(); err != nil {
		hasException = true
		return fmt.Errorf("job preparation failed: %v", err)
	}

	// 拆分任务
	readerTaskConfigs, writerTaskConfigs, err := jc.split()
	if err != nil {
		hasException = true
		return fmt.Errorf("job split failed: %v", err)
	}

	// 设置总阶段数
	jc.totalStage = len(readerTaskConfigs)

	// 初始化SchedulerReporter进行实时汇报
	reportInterval := time.Duration(jc.configuration.GetIntWithDefault("core.container.job.reportInterval", 30000)) * time.Millisecond
	sleepInterval := time.Duration(jc.configuration.GetIntWithDefault("core.container.job.sleepInterval", 10000)) * time.Millisecond
	jc.schedulerReporter = statistics.NewSchedulerReporter(jc.communicator, reportInterval, sleepInterval, jc.totalStage)

	// 启动实时汇报
	jc.schedulerReporter.Start()

	// 记录传输开始时间
	jc.startTransferTimeStamp = time.Now().UnixMilli()

	// 调度执行
	if err = jc.schedule(readerTaskConfigs, writerTaskConfigs); err != nil {
		hasException = true
		return fmt.Errorf("job schedule failed: %v", err)
	}

	// 记录传输结束时间
	jc.endTransferTimeStamp = time.Now().UnixMilli()

	// 后处理
	if err = jc.post(); err != nil {
		hasException = true
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

	// 为TaskGroup注册Communication
	for i := 0; i < taskCount; i++ {
		communication := statistics.NewCommunication()
		jc.communicator.RegisterCommunication(i, communication)
	}

	// 创建TaskGroup并发执行
	var wg sync.WaitGroup
	errChan := make(chan error, taskCount)

	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			defer func() {
				// 标记任务完成
				if comm := jc.communicator.GetCommunication(index); comm != nil {
					comm.IncreaseCounter(statistics.STAGE, 1)
					if comm.GetState() != statistics.StateFailed {
						comm.SetState(statistics.StateSucceeded)
					}
				}
			}()

			taskGroupConfig := jc.configuration.Clone()
			taskGroupConfig.Set("taskGroup.id", index)
			taskGroupConfig.Set("taskGroup.reader", readerTaskConfigs[index].Get(""))
			taskGroupConfig.Set("taskGroup.writer", writerTaskConfigs[index].Get(""))

			taskGroupContainer := taskgroup.NewTaskGroupContainer(taskGroupConfig, index)
			if err := taskGroupContainer.Start(readerTaskConfigs[index], writerTaskConfigs[index]); err != nil {
				// 标记任务失败
				if comm := jc.communicator.GetCommunication(index); comm != nil {
					comm.SetState(statistics.StateFailed)
					comm.SetThrowable(err)
				}
				errChan <- fmt.Errorf("taskGroup %d failed: %v", index, err)
			} else {
				// 任务成功，合并TaskGroupContainer的统计信息到JobContainer
				if jobComm := jc.communicator.GetCommunication(index); jobComm != nil && taskGroupContainer != nil {
					// 获取TaskGroupContainer的Communication并合并
					if tgComm := taskGroupContainer.GetCommunication(); tgComm != nil {
						jobComm.MergeFrom(tgComm)
					}
				}
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

// logStatistics 输出最终统计信息
func (jc *JobContainer) logStatistics() {
	totalCosts := (jc.endTimeStamp - jc.startTimeStamp) / 1000
	transferCosts := (jc.endTransferTimeStamp - jc.startTransferTimeStamp) / 1000
	if transferCosts <= 0 {
		transferCosts = 1
	}

	if jc.communicator == nil {
		return
	}

	communication := jc.communicator.Collect()
	communication.SetTimestamp(jc.endTimeStamp)

	// 计算速度
	byteSpeedPerSecond := communication.GetLongCounter(statistics.READ_SUCCEED_BYTES) / transferCosts
	recordSpeedPerSecond := communication.GetLongCounter(statistics.READ_SUCCEED_RECORDS) / transferCosts

	communication.SetLongCounter(statistics.BYTE_SPEED, byteSpeedPerSecond)
	communication.SetLongCounter(statistics.RECORD_SPEED, recordSpeedPerSecond)

	// 汇报最终统计
	jc.communicator.Report(communication)

	appLogger := logger.App()
	appLogger.Info("=================================== Final Job Statistics ===================================")
	appLogger.Info("Job Start Time", zap.Time("startTime", time.UnixMilli(jc.startTimeStamp)))
	appLogger.Info("Job End Time", zap.Time("endTime", time.UnixMilli(jc.endTimeStamp)))
	appLogger.Info("Job Total Cost", zap.String("totalCosts", fmt.Sprintf("%ds", totalCosts)))
	appLogger.Info("Job Average Throughput", zap.String("throughput", statistics.DefaultCommunicationTool.FormatBytes(byteSpeedPerSecond)+"/s"))
	appLogger.Info("Record Write Speed", zap.String("recordSpeed", fmt.Sprintf("%d rec/s", recordSpeedPerSecond)))
	appLogger.Info("Total Read Records", zap.Int64("totalReadRecords", statistics.DefaultCommunicationTool.GetTotalReadRecords(communication)))
	appLogger.Info("Total Error Records", zap.Int64("totalErrorRecords", statistics.DefaultCommunicationTool.GetTotalErrorRecords(communication)))

	// 输出Transformer统计
	if communication.GetLongCounter(statistics.TRANSFORMER_SUCCEED_RECORDS) > 0 ||
		communication.GetLongCounter(statistics.TRANSFORMER_FAILED_RECORDS) > 0 ||
		communication.GetLongCounter(statistics.TRANSFORMER_FILTER_RECORDS) > 0 {
		appLogger.Info("Transformer Success Records", zap.Int64("records", communication.GetLongCounter(statistics.TRANSFORMER_SUCCEED_RECORDS)))
		appLogger.Info("Transformer Failed Records", zap.Int64("records", communication.GetLongCounter(statistics.TRANSFORMER_FAILED_RECORDS)))
		appLogger.Info("Transformer Filter Records", zap.Int64("records", communication.GetLongCounter(statistics.TRANSFORMER_FILTER_RECORDS)))
		appLogger.Info("Transformer Used Time", zap.String("usedTime", statistics.DefaultCommunicationTool.FormatTime(communication.GetLongCounter(statistics.TRANSFORMER_USED_TIME))))
	}

	appLogger.Info("==============================================================================================")
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