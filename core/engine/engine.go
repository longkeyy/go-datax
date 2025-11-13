package engine

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/longkeyy/go-datax/common/config"
	coreconfig "github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/core/job"
	"go.uber.org/zap"
)

// Engine is now stateless - just a namespace for execution functions
type Engine struct{}

func NewEngine() *Engine {
	return &Engine{}
}

// Start executes a job synchronously (blocking) - for backward compatibility
func (e *Engine) Start(allConf config.Configuration) error {
	if allConf == nil {
		return fmt.Errorf("configuration cannot be nil")
	}

	// Create and run job using new Job API
	j := job.NewJob(allConf)
	return j.Run(context.Background())
}

func Main(ver string) {
	// Setup structured logging with zap
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

	appLogger := logger.App()
	appLogger.Info("DataX Go starting",
		zap.String("version", ver))

	var jobPath string
	flag.StringVar(&jobPath, "job", "", "Job configuration file path")
	flag.Parse()

	if jobPath == "" {
		fmt.Println("Usage: datax -job <config-file>")
		os.Exit(1)
	}

	appLogger.Info("Starting DataX execution", zap.String("jobPath", jobPath))

	configuration, err := coreconfig.FromFile(jobPath)
	if err != nil {
		appLogger.Error("Failed to parse configuration file", zap.Error(err))
		os.Exit(1)
	}

	// Log configuration for debugging (sensitive data already filtered)
	if jsonStr, err := configuration.ToJSON(); err == nil {
		appLogger.Debug("Job configuration loaded", zap.String("config", jsonStr))
	}

	engine := NewEngine()
	if err := engine.Start(configuration); err != nil {
		appLogger.Error("DataX execution failed", zap.Error(err))
		os.Exit(1)
	}

	appLogger.Info("DataX execution completed successfully")
}
