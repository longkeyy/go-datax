package engine

import (
	"flag"
	"fmt"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/core/job"
	"log"
	"os"
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
	if jsonStr, err := configuration.ToJSON(); err == nil {
		log.Printf("Job configuration:\n%s", jsonStr)
	}

	// 启动引擎
	return e.Start(configuration)
}

func Main() {
	var jobPath string
	flag.StringVar(&jobPath, "job", "", "Job configuration file path")
	flag.Parse()

	if jobPath == "" {
		log.Fatal("job configuration file path is required, use -job <path>")
	}

	engine := NewEngine()
	args := []string{"-job", jobPath}

	if err := engine.Entry(args); err != nil {
		log.Fatalf("DataX execution failed: %v", err)
		os.Exit(1)
	}

	log.Println("DataX execution completed successfully")
}