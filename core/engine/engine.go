package engine

import (
	"flag"
	"fmt"
	"os"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/core/job"
	"go.uber.org/zap"
)

// Engine DataX引擎入口类
type Engine struct {
	configuration *config.Configuration
}

func NewEngine() *Engine {
	return &Engine{}
}

func (e *Engine) Start(allConf *config.Configuration) error {
	if allConf == nil {
		return fmt.Errorf("configuration cannot be nil")
	}

	e.configuration = allConf

	// 检查是否为Job模式
	isJob := allConf.GetStringWithDefault("core.container.model", "job") == "job"

	if isJob {
		// 创建JobContainer并启动
		jobContainer := job.NewJobContainer(allConf)
		return jobContainer.Start()
	} else {
		// TaskGroup模式暂不实现
		return fmt.Errorf("taskGroup mode not implemented yet")
	}
}

func (e *Engine) Entry(args []string) error {
	// 解析命令行参数
	jobPath := ""
	if len(args) > 0 {
		for i, arg := range args {
			if arg == "-job" && i+1 < len(args) {
				jobPath = args[i+1]
				break
			}
		}
	}

	if jobPath == "" {
		return fmt.Errorf("job configuration file path is required, use -job <path>")
	}

	// 解析配置文件
	configuration, err := config.FromFile(jobPath)
	if err != nil {
		return fmt.Errorf("failed to parse configuration file: %v", err)
	}

	// 打印配置信息（过滤敏感信息）
	appLogger := logger.App()
	if jsonStr, err := configuration.ToJSON(); err == nil {
		appLogger.Debug("Job configuration loaded", zap.String("config", jsonStr))
	}

	// 启动引擎
	return e.Start(configuration)
}

func Main(ver string) {
	// 初始化zap日志器
	logConfig := &logger.LoggerConfig{
		Level:       logger.LevelInfo,
		Development: true,
		Console:     true,
	}

	if err := logger.Initialize(logConfig); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// 使用应用级日志器
	appLogger := logger.App()
	appLogger.Info("DataX Go starting",
		zap.String("version", ver))

	var jobPath string
	flag.StringVar(&jobPath, "job", "", "Job configuration file path")
	flag.Parse()

	if jobPath == "" {
		appLogger.Error("Job configuration file path is required")
		fmt.Println("Usage: datax -job <config-file>")
		os.Exit(1)
	}

	appLogger.Info("Starting DataX execution", zap.String("jobPath", jobPath))

	engine := NewEngine()
	args := []string{"-job", jobPath}

	if err := engine.Entry(args); err != nil {
		appLogger.Error("DataX execution failed", zap.Error(err))
		os.Exit(1)
	}

	appLogger.Info("DataX execution completed successfully")
}
