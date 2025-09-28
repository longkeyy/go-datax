package logger

import (
	"context"
)

// Context keys for task information
type contextKey string

const (
	TaskGroupIDKey contextKey = "taskGroupId"
	TaskIDKey      contextKey = "taskId"
	JobIDKey       contextKey = "jobId"
)

// WithTaskGroupID 在context中添加taskGroupId
func WithTaskGroupID(ctx context.Context, taskGroupId int) context.Context {
	return context.WithValue(ctx, TaskGroupIDKey, taskGroupId)
}

// WithTaskID 在context中添加taskId
func WithTaskID(ctx context.Context, taskId int) context.Context {
	return context.WithValue(ctx, TaskIDKey, taskId)
}

// WithJobID 在context中添加jobId
func WithJobID(ctx context.Context, jobId string) context.Context {
	return context.WithValue(ctx, JobIDKey, jobId)
}

// WithTaskContext 在context中添加完整的任务信息
func WithTaskContext(ctx context.Context, taskGroupId, taskId int, jobId string) context.Context {
	ctx = WithTaskGroupID(ctx, taskGroupId)
	ctx = WithTaskID(ctx, taskId)
	ctx = WithJobID(ctx, jobId)
	return ctx
}

// GetTaskGroupID 从context中获取taskGroupId
func GetTaskGroupID(ctx context.Context) (int, bool) {
	taskGroupId, ok := ctx.Value(TaskGroupIDKey).(int)
	return taskGroupId, ok
}

// GetTaskID 从context中获取taskId
func GetTaskID(ctx context.Context) (int, bool) {
	taskId, ok := ctx.Value(TaskIDKey).(int)
	return taskId, ok
}

// GetJobID 从context中获取jobId
func GetJobID(ctx context.Context) (string, bool) {
	jobId, ok := ctx.Value(JobIDKey).(string)
	return jobId, ok
}

// TaskLoggerFromContext 从context创建带任务信息的日志器
func TaskLoggerFromContext(ctx context.Context) TaskLogger {
	logger := Task()

	if taskGroupId, ok := GetTaskGroupID(ctx); ok {
		logger = logger.WithTaskGroup(taskGroupId)
	}

	if taskId, ok := GetTaskID(ctx); ok {
		logger = logger.WithTaskId(taskId)
	}

	return logger
}