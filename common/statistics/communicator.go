package statistics

import (
	"context"
	"sync"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/logger"
	"go.uber.org/zap"
)

// Communicator 通信器接口，负责收集和汇报统计信息
type Communicator interface {
	// RegisterCommunication 注册Communication
	RegisterCommunication(taskId int, communication *Communication)

	// GetCommunication 获取指定task的Communication
	GetCommunication(taskId int) *Communication

	// Collect 收集所有Communication信息
	Collect() *Communication

	// Report 汇报Communication信息
	Report(communication *Communication)

	// ResetCommunication 重置指定task的Communication
	ResetCommunication(taskId int)
}

// AbstractCommunicator 抽象通信器基类
type AbstractCommunicator struct {
	mu            sync.RWMutex
	communications map[int]*Communication
	jobId         int64
	configuration config.Configuration
}

func NewAbstractCommunicator(configuration config.Configuration) *AbstractCommunicator {
	jobId := configuration.GetLongWithDefault("core.container.job.id", 0)
	return &AbstractCommunicator{
		communications: make(map[int]*Communication),
		jobId:         jobId,
		configuration: configuration,
	}
}

// RegisterCommunication 注册Communication
func (ac *AbstractCommunicator) RegisterCommunication(taskId int, communication *Communication) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.communications[taskId] = communication
}

// GetCommunication 获取指定task的Communication
func (ac *AbstractCommunicator) GetCommunication(taskId int) *Communication {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	return ac.communications[taskId]
}

// Collect 收集所有Communication信息
func (ac *AbstractCommunicator) Collect() *Communication {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	result := NewCommunication()
	for _, comm := range ac.communications {
		if comm != nil {
			result.MergeFrom(comm)
		}
	}
	return result
}

// ResetCommunication 重置指定task的Communication
func (ac *AbstractCommunicator) ResetCommunication(taskId int) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	if comm := ac.communications[taskId]; comm != nil {
		comm.Reset()
	}
}

// GetJobId 获取JobId
func (ac *AbstractCommunicator) GetJobId() int64 {
	return ac.jobId
}

// GetConfiguration 获取配置
func (ac *AbstractCommunicator) GetConfiguration() config.Configuration {
	return ac.configuration
}

// JobCommunicator Job级别的通信器
type JobCommunicator struct {
	*AbstractCommunicator
	reporter *JobReporter
}

func NewJobCommunicator(configuration config.Configuration) *JobCommunicator {
	abstract := NewAbstractCommunicator(configuration)
	return &JobCommunicator{
		AbstractCommunicator: abstract,
		reporter:            NewJobReporter(abstract.jobId),
	}
}

// Report 汇报Job级别的Communication信息
func (jcc *JobCommunicator) Report(communication *Communication) {
	// 汇报给Reporter
	jcc.reporter.ReportJobCommunication(jcc.jobId, communication)

	// 输出进度快照
	snapshot := DefaultCommunicationTool.GetSnapshot(communication)
	logger.App().Info(snapshot)

	// 输出VM信息（可选）
	jcc.reportVMInfo()
}

func (jcc *JobCommunicator) reportVMInfo() {
	// 这里可以添加VM信息汇报逻辑，类似Java版本的VMInfo
	// 目前先简单实现
}

// TaskCommunicator TaskGroup级别的通信器
type TaskCommunicator struct {
	*AbstractCommunicator
	taskGroupId int
}

func NewTaskCommunicator(configuration config.Configuration, taskGroupId int) *TaskCommunicator {
	abstract := NewAbstractCommunicator(configuration)
	return &TaskCommunicator{
		AbstractCommunicator: abstract,
		taskGroupId:         taskGroupId,
	}
}

// Report 汇报TaskGroup级别的Communication信息
func (tgc *TaskCommunicator) Report(communication *Communication) {
	taskLogger := logger.TaskGroupLogger(tgc.taskGroupId)

	// 输出简化的进度信息
	snapshot := DefaultCommunicationTool.GetSnapshotStruct(communication)
	taskLogger.Info("TaskGroup progress update",
		zap.String("total", snapshot.Total),
		zap.String("speed", snapshot.Speed),
		zap.String("error", snapshot.Error),
		zap.String("percentage", snapshot.Percentage))
}

// JobReporter Job级别的汇报器
type JobReporter struct {
	jobId int64
}

func NewJobReporter(jobId int64) *JobReporter {
	return &JobReporter{jobId: jobId}
}

// ReportJobCommunication 汇报Job的Communication
func (jr *JobReporter) ReportJobCommunication(jobId int64, communication *Communication) {
	// 这里可以实现向外部系统汇报的逻辑
	// 例如发送到DataX服务端、写入数据库等
	// 目前先记录日志
	appLogger := logger.App()

	if jsonSnapshot, err := DefaultCommunicationTool.GetJSONSnapshot(communication); err == nil {
		appLogger.Debug("Job communication report",
			zap.Int64("jobId", jobId),
			zap.String("snapshot", jsonSnapshot))
	}
}

// SchedulerReporter 调度器级别的汇报器，负责定期汇报进度
type SchedulerReporter struct {
	communicator     Communicator
	reportInterval   time.Duration
	sleepInterval    time.Duration
	totalTasks       int
	ctx              context.Context
	cancel           context.CancelFunc
	lastCommunication *Communication
	mu               sync.RWMutex
}

func NewSchedulerReporter(communicator Communicator, reportInterval, sleepInterval time.Duration, totalTasks int) *SchedulerReporter {
	ctx, cancel := context.WithCancel(context.Background())
	return &SchedulerReporter{
		communicator:      communicator,
		reportInterval:    reportInterval,
		sleepInterval:     sleepInterval,
		totalTasks:        totalTasks,
		ctx:               ctx,
		cancel:            cancel,
		lastCommunication: NewCommunication(),
	}
}

// Start 启动定期汇报
func (sr *SchedulerReporter) Start() {
	go sr.reportLoop()
}

// Stop 停止定期汇报
func (sr *SchedulerReporter) Stop() {
	sr.cancel()
}

func (sr *SchedulerReporter) reportLoop() {
	ticker := time.NewTicker(sr.sleepInterval)
	defer ticker.Stop()

	lastReportTime := time.Now()
	appLogger := logger.App()

	for {
		select {
		case <-sr.ctx.Done():
			return
		case <-ticker.C:
			// 收集当前状态
			nowCommunication := sr.communicator.Collect()
			nowCommunication.SetTimestamp(time.Now().UnixMilli())

			appLogger.Debug("Communication collected",
				zap.String("state", nowCommunication.GetState().String()))

			// 检查是否需要汇报
			now := time.Now()
			if now.Sub(lastReportTime) > sr.reportInterval {
				sr.mu.Lock()
				reportCommunication := DefaultCommunicationTool.GetReportCommunication(
					nowCommunication, sr.lastCommunication, sr.totalTasks)
				sr.lastCommunication = nowCommunication
				sr.mu.Unlock()

				sr.communicator.Report(reportCommunication)
				lastReportTime = now
			}

			// 检查是否完成
			if nowCommunication.GetState() == StateSucceeded {
				appLogger.Info("Scheduler accomplished all tasks.")
				return
			}

			// 检查是否失败
			if nowCommunication.GetState() == StateFailed {
				appLogger.Error("Scheduler failed",
					zap.String("error", nowCommunication.GetThrowableMessage()))
				return
			}
		}
	}
}