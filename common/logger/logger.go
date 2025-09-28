package logger

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	globalLogger *Logger
	once         sync.Once
)

// LogLevel 日志级别
type LogLevel string

const (
	LevelDebug LogLevel = "debug"
	LevelInfo  LogLevel = "info"
	LevelWarn  LogLevel = "warn"
	LevelError LogLevel = "error"
)

// LoggerConfig 日志配置
type LoggerConfig struct {
	Level      LogLevel `json:"level"`
	OutputPath string   `json:"output_path"`
	// 开发模式：更易读的格式，生产模式：JSON格式
	Development bool `json:"development"`
	// 是否输出到控制台
	Console bool `json:"console"`
}

// DefaultConfig 默认配置
func DefaultConfig() *LoggerConfig {
	return &LoggerConfig{
		Level:       LevelInfo,
		OutputPath:  "",
		Development: true,
		Console:     true,
	}
}

// Logger 全局日志管理器
type Logger struct {
	appLogger       *zap.Logger // 应用级别日志
	componentLogger *zap.Logger // 组件级别日志
	taskLogger      *zap.Logger // 任务级别日志
	config          *LoggerConfig
}

// Initialize 初始化全局日志管理器
func Initialize(config *LoggerConfig) error {
	var err error
	once.Do(func() {
		if config == nil {
			config = DefaultConfig()
		}
		globalLogger, err = newLogger(config)
	})
	return err
}

// newLogger 创建新的日志实例
func newLogger(config *LoggerConfig) (*Logger, error) {
	// 构建zap配置
	zapConfig := zap.NewProductionConfig()
	if config.Development {
		zapConfig = zap.NewDevelopmentConfig()
		zapConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	// 明确控制堆栈追踪：只在Error级别及以上显示堆栈
	zapConfig.DisableStacktrace = false
	zapConfig.EncoderConfig.StacktraceKey = "stacktrace"

	// 设置日志级别
	switch config.Level {
	case LevelDebug:
		zapConfig.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	case LevelInfo:
		zapConfig.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	case LevelWarn:
		zapConfig.Level = zap.NewAtomicLevelAt(zapcore.WarnLevel)
	case LevelError:
		zapConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	default:
		zapConfig.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	}

	// 设置输出路径
	var outputPaths []string
	if config.Console {
		outputPaths = append(outputPaths, "stdout")
	}
	if config.OutputPath != "" {
		outputPaths = append(outputPaths, config.OutputPath)
	}
	if len(outputPaths) == 0 {
		outputPaths = []string{"stdout"}
	}
	zapConfig.OutputPaths = outputPaths
	zapConfig.ErrorOutputPaths = outputPaths

	// 自定义时间格式和消息格式，兼容Java版本
	zapConfig.EncoderConfig.TimeKey = "timestamp"
	zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zapConfig.EncoderConfig.MessageKey = "message"
	zapConfig.EncoderConfig.LevelKey = "level"

	// 禁用调用者信息（文件:行号），因为总是指向logger包内部，没有用处
	zapConfig.DisableCaller = true

	// 构建基础logger，只在Error级别及以上显示堆栈追踪
	baseLogger, err := zapConfig.Build(zap.AddStacktrace(zapcore.ErrorLevel))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %v", err)
	}

	logger := &Logger{
		config: config,
	}

	// 创建三层日志器：应用级、组件级、任务级
	logger.appLogger = baseLogger.Named("APP")
	logger.componentLogger = baseLogger.Named("COMPONENT")
	logger.taskLogger = baseLogger.Named("TASK")

	return logger, nil
}

// GetLogger 获取全局日志器
func GetLogger() *Logger {
	if globalLogger == nil {
		// 如果未初始化，使用默认配置
		Initialize(DefaultConfig())
	}
	return globalLogger
}

// ApplicationLogger 应用级日志接口
type ApplicationLogger interface {
	Info(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
	Debug(msg string, fields ...zap.Field)
}

// ComponentLogger 组件级日志接口
type ComponentLogger interface {
	Info(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
	Debug(msg string, fields ...zap.Field)
	WithComponent(component string) ComponentLogger
}

// TaskLogger 任务级日志接口 - 支持Java版本格式
type TaskLogger interface {
	Info(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
	Debug(msg string, fields ...zap.Field)
	// Java版本兼容方法：taskGroup[x] taskId[y] 格式
	WithTaskGroup(taskGroupId int) TaskLogger
	WithTaskId(taskId int) TaskLogger
	WithContext(ctx context.Context) TaskLogger
}

// App 获取应用级日志器
func (l *Logger) App() ApplicationLogger {
	return &appLogger{logger: l.appLogger}
}

// Component 获取组件级日志器
func (l *Logger) Component() ComponentLogger {
	return &componentLogger{logger: l.componentLogger}
}

// Task 获取任务级日志器
func (l *Logger) Task() TaskLogger {
	return &taskLogger{logger: l.taskLogger}
}

// 应用级日志器实现
type appLogger struct {
	logger *zap.Logger
}

func (a *appLogger) Info(msg string, fields ...zap.Field) {
	a.logger.Info(msg, fields...)
}

func (a *appLogger) Warn(msg string, fields ...zap.Field) {
	a.logger.Warn(msg, fields...)
}

func (a *appLogger) Error(msg string, fields ...zap.Field) {
	a.logger.Error(msg, fields...)
}

func (a *appLogger) Debug(msg string, fields ...zap.Field) {
	a.logger.Debug(msg, fields...)
}

// 组件级日志器实现
type componentLogger struct {
	logger *zap.Logger
}

func (c *componentLogger) Info(msg string, fields ...zap.Field) {
	c.logger.Info(msg, fields...)
}

func (c *componentLogger) Warn(msg string, fields ...zap.Field) {
	c.logger.Warn(msg, fields...)
}

func (c *componentLogger) Error(msg string, fields ...zap.Field) {
	c.logger.Error(msg, fields...)
}

func (c *componentLogger) Debug(msg string, fields ...zap.Field) {
	c.logger.Debug(msg, fields...)
}

func (c *componentLogger) WithComponent(component string) ComponentLogger {
	return &componentLogger{
		logger: c.logger.Named(component),
	}
}

// 任务级日志器实现
type taskLogger struct {
	logger *zap.Logger
}

func (t *taskLogger) Info(msg string, fields ...zap.Field) {
	t.logger.Info(msg, fields...)
}

func (t *taskLogger) Warn(msg string, fields ...zap.Field) {
	t.logger.Warn(msg, fields...)
}

func (t *taskLogger) Error(msg string, fields ...zap.Field) {
	t.logger.Error(msg, fields...)
}

func (t *taskLogger) Debug(msg string, fields ...zap.Field) {
	t.logger.Debug(msg, fields...)
}

func (t *taskLogger) WithTaskGroup(taskGroupId int) TaskLogger {
	return &taskLogger{
		logger: t.logger.With(zap.Int("taskGroup", taskGroupId)),
	}
}

func (t *taskLogger) WithTaskId(taskId int) TaskLogger {
	return &taskLogger{
		logger: t.logger.With(zap.Int("taskId", taskId)),
	}
}

func (t *taskLogger) WithContext(ctx context.Context) TaskLogger {
	// 从context中提取任务信息
	if taskGroupId, ok := ctx.Value("taskGroupId").(int); ok {
		if taskId, ok := ctx.Value("taskId").(int); ok {
			return &taskLogger{
				logger: t.logger.With(
					zap.Int("taskGroup", taskGroupId),
					zap.Int("taskId", taskId),
				),
			}
		}
		return &taskLogger{
			logger: t.logger.With(zap.Int("taskGroup", taskGroupId)),
		}
	}
	return t
}

// Sync 同步所有缓冲的日志
func (l *Logger) Sync() error {
	if err := l.appLogger.Sync(); err != nil {
		return err
	}
	if err := l.componentLogger.Sync(); err != nil {
		return err
	}
	if err := l.taskLogger.Sync(); err != nil {
		return err
	}
	return nil
}

// 全局便捷方法

// App 获取应用级日志器
func App() ApplicationLogger {
	return GetLogger().App()
}

// Component 获取组件级日志器
func Component() ComponentLogger {
	return GetLogger().Component()
}

// Task 获取任务级日志器
func Task() TaskLogger {
	return GetLogger().Task()
}

// Sync 同步所有日志
func Sync() error {
	return GetLogger().Sync()
}

// SetLevel 动态设置日志级别
func SetLevel(level LogLevel) {
	// 这里可以实现动态级别调整
	// 需要保存atomic level的引用
}

// 兼容Java版本的便捷方法

// TaskGroupLogger 创建带taskGroup上下文的日志器
func TaskGroupLogger(taskGroupId int) TaskLogger {
	return Task().WithTaskGroup(taskGroupId)
}

// TaskIdLogger 创建带taskId上下文的日志器
func TaskIdLogger(taskGroupId, taskId int) TaskLogger {
	return Task().WithTaskGroup(taskGroupId).WithTaskId(taskId)
}

// ComponentWithName 创建带组件名的日志器
func ComponentWithName(component string) ComponentLogger {
	return Component().WithComponent(component)
}
