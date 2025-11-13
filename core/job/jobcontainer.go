package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/statistics"
	"github.com/longkeyy/go-datax/core/task"
	coreplugin "github.com/longkeyy/go-datax/core/registry"
	"go.uber.org/zap"
)

// JobContainer manages the complete lifecycle of a data transfer job,
// coordinating between reader and writer plugins while providing
// real-time statistics and error handling.
type JobContainer struct {
	configuration   config.Configuration
	readerPlugin    string
	writerPlugin    string
	needChannels    int
	communicator    *statistics.JobCommunicator
	schedulerReporter *statistics.SchedulerReporter
	totalStage      int
	startTimeStamp  int64
	endTimeStamp    int64
	startTransferTimeStamp int64
	endTransferTimeStamp   int64

	// Context support for cancellation
	ctx    context.Context
	cancel context.CancelFunc
}

func NewJobContainer(configuration config.Configuration) *JobContainer {
	// Bridge new configuration format to legacy statistics system
	oldConfig := convertNewConfigToOldConfig(configuration)
	communicator := statistics.NewJobCommunicator(oldConfig)
	return &JobContainer{
		configuration: configuration,
		communicator:  communicator,
	}
}

// convertNewConfigToOldConfig provides compatibility bridge for statistics system.
// Returns config directly since we now use unified interfaces.
func convertNewConfigToOldConfig(newConfig config.Configuration) config.Configuration {
	return newConfig
}

// Start executes job synchronously without context support (for backward compatibility)
func (jc *JobContainer) Start() error {
	return jc.StartWithContext(context.Background())
}

// StartWithContext executes job with context support for cancellation
func (jc *JobContainer) StartWithContext(ctx context.Context) error {
	// Setup context with cancellation
	jc.ctx, jc.cancel = context.WithCancel(ctx)
	defer jc.cancel()

	appLogger := logger.App()
	appLogger.Info("DataX JobContainer starts job")

	startTime := time.Now()
	jc.startTimeStamp = startTime.UnixMilli()
	var err error
	hasException := false

	defer func() {
		// Ensure scheduler reporter cleanup on function exit
		if jc.schedulerReporter != nil {
			jc.schedulerReporter.Stop()
		}

		endTime := time.Now()
		jc.endTimeStamp = endTime.UnixMilli()
		duration := endTime.Sub(startTime)

		if !hasException {
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

	if err = jc.init(); err != nil {
		hasException = true
		return fmt.Errorf("job initialization failed: %v", err)
	}

	if err = jc.prepare(); err != nil {
		hasException = true
		return fmt.Errorf("job preparation failed: %v", err)
	}

	readerTaskConfigs, writerTaskConfigs, err := jc.split()
	if err != nil {
		hasException = true
		return fmt.Errorf("job split failed: %v", err)
	}

	jc.totalStage = len(readerTaskConfigs)

	// Setup real-time progress reporting with configurable intervals
	reportInterval := time.Duration(jc.configuration.GetIntWithDefault("core.container.job.reportInterval", 30000)) * time.Millisecond
	sleepInterval := time.Duration(jc.configuration.GetIntWithDefault("core.container.job.sleepInterval", 10000)) * time.Millisecond
	jc.schedulerReporter = statistics.NewSchedulerReporter(jc.communicator, reportInterval, sleepInterval, jc.totalStage)

	jc.schedulerReporter.Start()

	jc.startTransferTimeStamp = time.Now().UnixMilli()

	if err = jc.schedule(readerTaskConfigs, writerTaskConfigs); err != nil {
		hasException = true
		return fmt.Errorf("job schedule failed: %v", err)
	}

	jc.endTransferTimeStamp = time.Now().UnixMilli()

	if err = jc.post(); err != nil {
		hasException = true
		return fmt.Errorf("job post processing failed: %v", err)
	}

	return nil
}

func (jc *JobContainer) init() error {
	contentList := jc.configuration.GetListConfiguration("job.content")
	if len(contentList) == 0 {
		return fmt.Errorf("job.content is empty")
	}

	// Currently limited to single content configuration for simplicity
	content := contentList[0]
	jc.readerPlugin = content.GetString("reader.name")
	jc.writerPlugin = content.GetString("writer.name")

	if jc.readerPlugin == "" {
		return fmt.Errorf("reader plugin name is required")
	}
	if jc.writerPlugin == "" {
		return fmt.Errorf("writer plugin name is required")
	}

	// Configure parallel channels, defaulting to 1 for safety
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
	// Prepare phase allows writer to setup tables, validate schema, etc.
	contentList := jc.configuration.GetListConfiguration("job.content")
	content := contentList[0]

	writerJob, err := jc.createWriterJob(content.GetConfiguration("writer"))
	if err != nil {
		return fmt.Errorf("failed to create writer job for prepare: %v", err)
	}

	if err := writerJob.Prepare(); err != nil {
		return fmt.Errorf("writer job prepare failed: %v", err)
	}

	return nil
}

func (jc *JobContainer) split() ([]config.Configuration, []config.Configuration, error) {
	contentList := jc.configuration.GetListConfiguration("job.content")
	content := contentList[0]

	readerJob, err := jc.createReaderJob(content.GetConfiguration("reader"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create reader job: %v", err)
	}

	readerTaskConfigs, err := readerJob.Split(jc.needChannels)
	if err != nil {
		return nil, nil, fmt.Errorf("reader split failed: %v", err)
	}

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

func (jc *JobContainer) schedule(readerTaskConfigs, writerTaskConfigs []config.Configuration) error {
	if len(readerTaskConfigs) != len(writerTaskConfigs) {
		return fmt.Errorf("reader tasks (%d) and writer tasks (%d) count mismatch",
			len(readerTaskConfigs), len(writerTaskConfigs))
	}

	taskCount := len(readerTaskConfigs)
	appLogger := logger.App()
	appLogger.Info("Starting task groups", zap.Int("taskGroups", taskCount))

	// Pre-register communication channels for each task group to enable statistics collection
	for i := 0; i < taskCount; i++ {
		communication := statistics.NewCommunication()
		jc.communicator.RegisterCommunication(i, communication)
	}

	// Execute task groups concurrently with proper error handling and statistics merging
	var wg sync.WaitGroup
	errChan := make(chan error, taskCount)

	for i := 0; i < taskCount; i++ {
		// Check for context cancellation before starting new task group
		select {
		case <-jc.ctx.Done():
			// Context cancelled, wait for already started tasks and return
			wg.Wait()
			close(errChan)
			return jc.ctx.Err()
		default:
		}

		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			defer func() {
				// Mark task completion and update statistics regardless of success/failure
				if comm := jc.communicator.GetCommunication(index); comm != nil {
					comm.IncreaseCounter(statistics.STAGE, 1)
					if comm.GetState() != statistics.StateFailed {
						comm.SetState(statistics.StateSucceeded)
					}
				}
			}()

			taskGroupConfig := jc.configuration.Clone()
			taskGroupConfig.Set("taskGroup.id", index)
			taskGroupConfig.Set("taskGroup.reader", readerTaskConfigs[index])
			taskGroupConfig.Set("taskGroup.writer", writerTaskConfigs[index])

			taskGroupContainer := task.NewTaskGroupContainer(taskGroupConfig, index)
			if err := taskGroupContainer.Start(readerTaskConfigs[index], writerTaskConfigs[index]); err != nil {
				if comm := jc.communicator.GetCommunication(index); comm != nil {
					comm.SetState(statistics.StateFailed)
					comm.SetThrowable(err)
				}
				errChan <- fmt.Errorf("taskGroup %d failed: %v", index, err)
			} else {
				// Merge task group statistics into job-level statistics for reporting
				if jobComm := jc.communicator.GetCommunication(index); jobComm != nil && taskGroupContainer != nil {
					if tgComm := taskGroupContainer.GetCommunication(); tgComm != nil {
						jobComm.MergeFrom(tgComm)
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	// Fail fast if any task group encountered errors
	for err := range errChan {
		return err
	}

	return nil
}

func (jc *JobContainer) post() error {
	// Reserved for cleanup operations like connection closing, temp file removal
	return nil
}

// logStatistics outputs comprehensive job execution metrics including
// throughput, timing, and error statistics for monitoring and debugging.
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

	// Calculate throughput metrics based on actual transfer time to avoid division by zero
	byteSpeedPerSecond := communication.GetLongCounter(statistics.READ_SUCCEED_BYTES) / transferCosts
	recordSpeedPerSecond := communication.GetLongCounter(statistics.READ_SUCCEED_RECORDS) / transferCosts

	communication.SetLongCounter(statistics.BYTE_SPEED, byteSpeedPerSecond)
	communication.SetLongCounter(statistics.RECORD_SPEED, recordSpeedPerSecond)

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

	// Include transformer metrics only when transformations were actually performed
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

func (jc *JobContainer) createReaderJob(readerConfig config.Configuration) (plugin.ReaderJob, error) {
	readerName := readerConfig.GetString("name")
	readerJobFactory, err := coreplugin.GetReaderJobFactory(readerName)
	if err != nil {
		return nil, err
	}

	readerJob := readerJobFactory.CreateReaderJob()
	if err := readerJob.Init(readerConfig); err != nil {
		return nil, fmt.Errorf("failed to init reader job: %v", err)
	}

	return readerJob, nil
}

func (jc *JobContainer) createWriterJob(writerConfig config.Configuration) (plugin.WriterJob, error) {
	writerName := writerConfig.GetString("name")
	writerJobFactory, err := coreplugin.GetWriterJobFactory(writerName)
	if err != nil {
		return nil, err
	}

	writerJob := writerJobFactory.CreateWriterJob()
	if err := writerJob.Init(writerConfig); err != nil {
		return nil, fmt.Errorf("failed to init writer job: %v", err)
	}

	return writerJob, nil
}

// MockReaderJob provides a test implementation for reader plugins.
// Used for testing job container logic without external dependencies.
type MockReaderJob struct {
	config config.Configuration
}

func (m *MockReaderJob) Init(config config.Configuration) error {
	m.config = config
	return nil
}

func (m *MockReaderJob) Split(adviceNumber int) ([]config.Configuration, error) {
	// Uniform split strategy - each task gets identical configuration with unique task ID
	configs := make([]config.Configuration, adviceNumber)
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

// MockWriterJob provides a test implementation for writer plugins.
// Used for testing job container logic without external dependencies.
type MockWriterJob struct {
	config config.Configuration
}

func (m *MockWriterJob) Init(config config.Configuration) error {
	m.config = config
	return nil
}

func (m *MockWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	// Uniform split strategy - each task gets identical configuration with unique task ID
	configs := make([]config.Configuration, mandatoryNumber)
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