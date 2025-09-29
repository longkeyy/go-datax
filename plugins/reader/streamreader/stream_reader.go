package streamreader

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/factory"
	"go.uber.org/zap"
)

// StreamReaderJob Stream读取作业，用于生成测试数据
type StreamReaderJob struct {
	config           config.Configuration
	sliceRecordCount int64
	columns          []map[string]interface{}
	factory          *factory.DataXFactory
}

func NewStreamReaderJob() *StreamReaderJob {
	return &StreamReaderJob{
		factory: factory.GetGlobalFactory(),
	}
}

func (job *StreamReaderJob) Init(config config.Configuration) error {
	job.config = config

	// 获取每个分片的记录数
	job.sliceRecordCount = int64(config.GetInt("parameter.sliceRecordCount"))
	if job.sliceRecordCount <= 0 {
		return fmt.Errorf("sliceRecordCount must be greater than 0")
	}

	// 获取列配置
	columnsConfig := config.GetList("parameter.column")
	if len(columnsConfig) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	// 解析列配置
	for _, columnConfig := range columnsConfig {
		if column, ok := columnConfig.(map[string]interface{}); ok {
			// 设置默认类型为string
			if _, exists := column["type"]; !exists {
				column["type"] = "string"
			}
			job.columns = append(job.columns, column)
		}
	}

	if len(job.columns) == 0 {
		return fmt.Errorf("no valid columns configured")
	}

	logger.Component().WithComponent("StreamReader").Info("StreamReader initialized",
		zap.Int64("sliceRecordCount", job.sliceRecordCount),
		zap.Int("columnCount", len(job.columns)))
	return nil
}

func (job *StreamReaderJob) Split(adviceNumber int) ([]config.Configuration, error) {
	taskConfigs := make([]config.Configuration, 0)

	// 创建指定数量的任务配置
	for i := 0; i < adviceNumber; i++ {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	logger.Component().WithComponent("StreamReader").Info("Split into tasks",
		zap.Int("taskCount", len(taskConfigs)),
		zap.Int64("recordsPerTask", job.sliceRecordCount))
	return taskConfigs, nil
}

func (job *StreamReaderJob) Post() error {
	return nil
}

func (job *StreamReaderJob) Destroy() error {
	return nil
}

// StreamReaderTask Stream读取任务
type StreamReaderTask struct {
	config    config.Configuration
	readerJob *StreamReaderJob
	rand      *rand.Rand
	factory   *factory.DataXFactory
}

func NewStreamReaderTask() *StreamReaderTask {
	return &StreamReaderTask{
		rand:    rand.New(rand.NewSource(time.Now().UnixNano())),
		factory: factory.GetGlobalFactory(),
	}
}

func (task *StreamReaderTask) Init(config config.Configuration) error {
	task.config = config

	// 创建ReaderJob来重用配置逻辑
	task.readerJob = NewStreamReaderJob()
	err := task.readerJob.Init(config)
	if err != nil {
		return err
	}

	return nil
}

func (task *StreamReaderTask) StartRead(recordSender plugin.RecordSender) error {
	logger.Component().WithComponent("StreamReader").Info("Starting to generate records",
		zap.Int64("recordCount", task.readerJob.sliceRecordCount))

	for i := int64(0); i < task.readerJob.sliceRecordCount; i++ {
		record := task.factory.GetRecordFactory().CreateRecord()

		// 为每一列生成数据
		for _, columnConfig := range task.readerJob.columns {
			column := task.generateColumnValue(columnConfig)
			record.AddColumn(column)
		}

		// 发送记录
		if err := recordSender.SendRecord(record); err != nil {
			return fmt.Errorf("failed to send record: %v", err)
		}

		// 每1000条记录输出一次进度
		if (i+1)%1000 == 0 {
			logger.Component().WithComponent("StreamReader").Info("Generation progress",
				zap.Int64("generated", i+1),
				zap.Int64("total", task.readerJob.sliceRecordCount))
		}
	}

	logger.Component().WithComponent("StreamReader").Info("Generation completed",
		zap.Int64("totalRecords", task.readerJob.sliceRecordCount))
	return nil
}

func (task *StreamReaderTask) generateColumnValue(columnConfig map[string]interface{}) element.Column {
	columnType, _ := columnConfig["type"].(string)
	value, hasValue := columnConfig["value"]
	columnFactory := task.factory.GetColumnFactory()

	switch columnType {
	case "long":
		if hasValue {
			if strVal, ok := value.(string); ok {
				if intVal, err := strconv.ParseInt(strVal, 10, 64); err == nil {
					return columnFactory.CreateLongColumn(intVal)
				}
			}
			if intVal, ok := value.(int64); ok {
				return columnFactory.CreateLongColumn(intVal)
			}
			if floatVal, ok := value.(float64); ok {
				return columnFactory.CreateLongColumn(int64(floatVal))
			}
		}
		// 生成随机长整数
		return columnFactory.CreateLongColumn(task.rand.Int63n(1000000))

	case "double":
		if hasValue {
			if strVal, ok := value.(string); ok {
				if floatVal, err := strconv.ParseFloat(strVal, 64); err == nil {
					return columnFactory.CreateDoubleColumn(floatVal)
				}
			}
			if floatVal, ok := value.(float64); ok {
				return columnFactory.CreateDoubleColumn(floatVal)
			}
		}
		// 生成随机浮点数
		return columnFactory.CreateDoubleColumn(task.rand.Float64() * 1000)

	case "bool", "boolean":
		if hasValue {
			if strVal, ok := value.(string); ok {
				if boolVal, err := strconv.ParseBool(strVal); err == nil {
					return columnFactory.CreateBoolColumn(boolVal)
				}
			}
			if boolVal, ok := value.(bool); ok {
				return columnFactory.CreateBoolColumn(boolVal)
			}
		}
		// 生成随机布尔值
		return columnFactory.CreateBoolColumn(task.rand.Intn(2) == 1)

	case "date":
		if hasValue {
			if strVal, ok := value.(string); ok {
				if dateVal, err := time.Parse("2006-01-02 15:04:05", strVal); err == nil {
					return columnFactory.CreateDateColumn(dateVal)
				}
			}
		}
		// 生成随机日期（过去一年内）
		now := time.Now()
		randomDays := task.rand.Intn(365)
		randomDate := now.AddDate(0, 0, -randomDays)
		return columnFactory.CreateDateColumn(randomDate)

	case "bytes":
		if hasValue {
			if strVal, ok := value.(string); ok {
				return columnFactory.CreateBytesColumn([]byte(strVal))
			}
		}
		// 生成随机字节数组
		length := task.rand.Intn(20) + 5
		bytes := make([]byte, length)
		for i := range bytes {
			bytes[i] = byte(task.rand.Intn(256))
		}
		return columnFactory.CreateBytesColumn(bytes)

	default: // string
		if hasValue {
			if strVal, ok := value.(string); ok {
				return columnFactory.CreateStringColumn(strVal)
			}
		}
		// 生成随机字符串
		return columnFactory.CreateStringColumn(task.generateRandomString())
	}
}

func (task *StreamReaderTask) generateRandomString() string {
	// 生成随机长度的字符串（5-20个字符）
	length := task.rand.Intn(16) + 5
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[task.rand.Intn(len(charset))]
	}
	return string(result)
}

func (task *StreamReaderTask) Post() error {
	return nil
}

func (task *StreamReaderTask) Destroy() error {
	return nil
}