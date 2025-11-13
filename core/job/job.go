package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/statistics"
)

// JobState represents the lifecycle state of a job
type JobState string

const (
	JobStateCreated   JobState = "CREATED"
	JobStateRunning   JobState = "RUNNING"
	JobStateSucceeded JobState = "SUCCEEDED"
	JobStateFailed    JobState = "FAILED"
	JobStateCancelled JobState = "CANCELLED"
)

// Job represents an observable and controllable data transfer task.
// Following Go's exec.Cmd pattern: create, start (async), wait (block).
type Job struct {
	ID     string
	config config.Configuration

	// Heavy object: released after completion
	container *JobContainer

	// Lightweight state (kept after completion)
	startTime  time.Time
	endTime    time.Time
	state      JobState
	finalStats *StatsSummary

	// Synchronization
	done chan struct{}
	err  error
	mu   sync.RWMutex
}

// JobStatus provides a lightweight snapshot of job state
type JobStatus struct {
	ID        string           `json:"id"`
	State     JobState         `json:"state"`
	StartTime time.Time        `json:"startTime"`
	EndTime   time.Time        `json:"endTime,omitempty"`
	Progress  *ProgressInfo    `json:"progress,omitempty"`
	Stats     *StatsSummary    `json:"stats,omitempty"`
	Error     string           `json:"error,omitempty"`
}

// ProgressInfo represents real-time execution progress
type ProgressInfo struct {
	TotalRecords   int64   `json:"totalRecords"`
	SucceedRecords int64   `json:"succeedRecords"`
	FailedRecords  int64   `json:"failedRecords"`
	FilterRecords  int64   `json:"filterRecords"`
	TotalBytes     int64   `json:"totalBytes"`
	ByteSpeed      int64   `json:"byteSpeed"`
	RecordSpeed    int64   `json:"recordSpeed"`
	Percentage     float64 `json:"percentage"`
}

// StatsSummary is a lightweight final statistics summary (kept after job completes)
type StatsSummary struct {
	TotalRecords   int64         `json:"totalRecords"`
	SucceedRecords int64         `json:"succeedRecords"`
	FailedRecords  int64         `json:"failedRecords"`
	FilterRecords  int64         `json:"filterRecords"`
	TotalBytes     int64         `json:"totalBytes"`
	Duration       time.Duration `json:"duration"`
	AverageSpeed   float64       `json:"averageSpeed"`
}

// NewJob creates a new job instance (but does not start it)
func NewJob(cfg config.Configuration) *Job {
	jobID := generateJobID(cfg)

	// Inject job ID into configuration for tracing
	cfg.Set("core.container.job.id", jobID)

	return &Job{
		ID:        jobID,
		config:    cfg,
		container: NewJobContainer(cfg),
		state:     JobStateCreated,
		done:      make(chan struct{}),
	}
}

// Start begins job execution asynchronously (similar to exec.Cmd.Start)
func (j *Job) Start(ctx context.Context) error {
	j.mu.Lock()
	if j.state != JobStateCreated {
		j.mu.Unlock()
		return fmt.Errorf("job %s already started (state: %s)", j.ID, j.state)
	}
	j.state = JobStateRunning
	j.startTime = time.Now()
	j.mu.Unlock()

	go j.run(ctx)
	return nil
}

// Wait blocks until the job completes and returns the result (similar to exec.Cmd.Wait)
func (j *Job) Wait() error {
	<-j.done
	return j.err
}

// Run executes the job synchronously (blocking) - convenience method for CLI mode
func (j *Job) Run(ctx context.Context) error {
	if err := j.Start(ctx); err != nil {
		return err
	}
	return j.Wait()
}

// Done returns a channel that is closed when the job completes (Go idiom)
func (j *Job) Done() <-chan struct{} {
	return j.done
}

// Status returns current job status snapshot
func (j *Job) Status() JobStatus {
	j.mu.RLock()
	defer j.mu.RUnlock()

	status := JobStatus{
		ID:        j.ID,
		State:     j.state,
		StartTime: j.startTime,
		EndTime:   j.endTime,
	}

	if j.err != nil {
		status.Error = j.err.Error()
	}

	// If running, get real-time progress
	if j.state == JobStateRunning && j.container != nil {
		comm := j.container.communicator.Collect()
		status.Progress = commToProgress(comm)
	}

	// If completed, return cached final stats
	if j.finalStats != nil {
		status.Stats = j.finalStats
	}

	return status
}

// GetState returns current job state
func (j *Job) GetState() JobState {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.state
}

// run executes the job in a background goroutine
func (j *Job) run(ctx context.Context) {
	defer close(j.done)
	defer j.release()

	// Execute job container with context support
	j.err = j.container.StartWithContext(ctx)

	j.mu.Lock()
	j.endTime = time.Now()
	if ctx.Err() == context.Canceled {
		j.state = JobStateCancelled
	} else if j.err != nil {
		j.state = JobStateFailed
	} else {
		j.state = JobStateSucceeded
	}
	j.mu.Unlock()
}

// release frees heavy objects after job completion to allow GC
func (j *Job) release() {
	j.mu.Lock()
	defer j.mu.Unlock()

	// Cache lightweight final statistics
	if j.container != nil && j.container.communicator != nil {
		comm := j.container.communicator.Collect()
		j.finalStats = commToSummary(comm, j.endTime.Sub(j.startTime))
	}

	// Release heavy JobContainer reference
	j.container = nil
}

// commToProgress converts Communication to ProgressInfo
func commToProgress(comm *statistics.Communication) *ProgressInfo {
	if comm == nil {
		return nil
	}

	// Calculate percentage based on snapshot data
	snapshot := statistics.DefaultCommunicationTool.GetSnapshotStruct(comm)
	percentage := 0.0
	if snapshot.Percentage != "" {
		// Parse percentage string (e.g., "75.00%")
		fmt.Sscanf(snapshot.Percentage, "%f%%", &percentage)
	}

	return &ProgressInfo{
		TotalRecords:   statistics.DefaultCommunicationTool.GetTotalReadRecords(comm),
		SucceedRecords: statistics.DefaultCommunicationTool.GetWriteSucceedRecords(comm),
		FailedRecords:  statistics.DefaultCommunicationTool.GetTotalErrorRecords(comm),
		FilterRecords:  comm.GetLongCounter(statistics.TRANSFORMER_FILTER_RECORDS),
		TotalBytes:     statistics.DefaultCommunicationTool.GetTotalReadBytes(comm),
		ByteSpeed:      comm.GetLongCounter(statistics.BYTE_SPEED),
		RecordSpeed:    comm.GetLongCounter(statistics.RECORD_SPEED),
		Percentage:     percentage,
	}
}

// commToSummary converts Communication to lightweight StatsSummary
func commToSummary(comm *statistics.Communication, duration time.Duration) *StatsSummary {
	if comm == nil {
		return nil
	}

	totalRecords := statistics.DefaultCommunicationTool.GetTotalReadRecords(comm)
	succeedRecords := statistics.DefaultCommunicationTool.GetWriteSucceedRecords(comm)
	var avgSpeed float64
	if duration.Seconds() > 0 {
		avgSpeed = float64(succeedRecords) / duration.Seconds()
	}

	return &StatsSummary{
		TotalRecords:   totalRecords,
		SucceedRecords: succeedRecords,
		FailedRecords:  statistics.DefaultCommunicationTool.GetTotalErrorRecords(comm),
		FilterRecords:  comm.GetLongCounter(statistics.TRANSFORMER_FILTER_RECORDS),
		TotalBytes:     statistics.DefaultCommunicationTool.GetTotalReadBytes(comm),
		Duration:       duration,
		AverageSpeed:   avgSpeed,
	}
}

// generateJobID generates unique job identifier
func generateJobID(cfg config.Configuration) string {
	// Try to get job ID from config (if provided)
	if existingID := cfg.GetString("core.container.job.id"); existingID != "" {
		return existingID
	}

	// Generate new UUID
	return uuid.New().String()
}
