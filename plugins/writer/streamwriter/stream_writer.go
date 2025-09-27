package streamwriter

import (
	"bufio"
	"fmt"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const (
	DefaultFieldDelimiter = "\t"
)

// StreamWriterJob Stream写入作业，用于输出数据到控制台或文件
type StreamWriterJob struct {
	config          *config.Configuration
	print           bool
	path            string
	fileName        string
	fieldDelimiter  string
	encoding        string
}

func NewStreamWriterJob() *StreamWriterJob {
	return &StreamWriterJob{
		print:          true,
		fieldDelimiter: DefaultFieldDelimiter,
		encoding:       "UTF-8",
	}
}

func (job *StreamWriterJob) Init(config *config.Configuration) error {
	job.config = config

	// 获取配置参数
	job.print = config.GetBoolWithDefault("parameter.print", true)
	job.path = config.GetString("parameter.path")
	job.fileName = config.GetString("parameter.fileName")
	job.fieldDelimiter = config.GetStringWithDefault("parameter.fieldDelimiter", DefaultFieldDelimiter)
	job.encoding = config.GetStringWithDefault("parameter.encoding", "UTF-8")

	// 如果配置了文件路径，验证目录
	if job.path != "" && job.fileName != "" {
		if err := job.validatePath(); err != nil {
			return err
		}
	}

	log.Printf("StreamWriter initialized: print=%t, path=%s, fileName=%s", job.print, job.path, job.fileName)
	return nil
}

func (job *StreamWriterJob) validatePath() error {
	// 检查目录是否存在，不存在则创建
	if err := os.MkdirAll(job.path, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", job.path, err)
	}

	// 如果文件已存在，删除它
	if job.fileName != "" {
		fullPath := filepath.Join(job.path, job.fileName)
		if _, err := os.Stat(fullPath); err == nil {
			if err := os.Remove(fullPath); err != nil {
				return fmt.Errorf("failed to remove existing file %s: %v", fullPath, err)
			}
		}
	}

	return nil
}

func (job *StreamWriterJob) Prepare() error {
	return nil
}

func (job *StreamWriterJob) Split(adviceNumber int) ([]*config.Configuration, error) {
	taskConfigs := make([]*config.Configuration, 0)

	// 创建指定数量的任务配置
	for i := 0; i < adviceNumber; i++ {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	log.Printf("Split into %d tasks", len(taskConfigs))
	return taskConfigs, nil
}

func (job *StreamWriterJob) Post() error {
	return nil
}

func (job *StreamWriterJob) Destroy() error {
	return nil
}

// StreamWriterTask Stream写入任务
type StreamWriterTask struct {
	config         *config.Configuration
	writerJob      *StreamWriterJob
	writer         *bufio.Writer
	file           *os.File
	recordCount    int64
}

func NewStreamWriterTask() *StreamWriterTask {
	return &StreamWriterTask{}
}

func (task *StreamWriterTask) Init(config *config.Configuration) error {
	task.config = config

	// 创建WriterJob来重用配置逻辑
	task.writerJob = NewStreamWriterJob()
	err := task.writerJob.Init(config)
	if err != nil {
		return err
	}

	// 根据配置创建输出流
	if task.writerJob.path != "" && task.writerJob.fileName != "" {
		// 写入文件
		fullPath := filepath.Join(task.writerJob.path, task.writerJob.fileName)
		task.file, err = os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %v", fullPath, err)
		}
		task.writer = bufio.NewWriter(task.file)
		log.Printf("Writing to file: %s", fullPath)
	} else {
		// 写入标准输出
		task.writer = bufio.NewWriter(os.Stdout)
		log.Printf("Writing to stdout")
	}

	return nil
}

func (task *StreamWriterTask) Prepare() error {
	return nil
}

func (task *StreamWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	defer func() {
		if task.writer != nil {
			task.writer.Flush()
		}
		if task.file != nil {
			task.file.Close()
		}
	}()

	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == plugin.ErrChannelClosed {
				break
			}
			return fmt.Errorf("failed to receive record: %v", err)
		}

		if task.writerJob.print {
			// 将记录转换为字符串并写入
			recordStr := task.recordToString(record)
			if _, err := task.writer.WriteString(recordStr); err != nil {
				return fmt.Errorf("failed to write record: %v", err)
			}
		}

		task.recordCount++

		// 每1000条记录输出一次进度并刷新缓冲区
		if task.recordCount%1000 == 0 {
			task.writer.Flush()
			log.Printf("Written %d records", task.recordCount)
		}
	}

	log.Printf("Total records written: %d", task.recordCount)
	return nil
}

func (task *StreamWriterTask) recordToString(record element.Record) string {
	columnCount := record.GetColumnNumber()
	if columnCount == 0 {
		return "\n"
	}

	values := make([]string, columnCount)
	for i := 0; i < columnCount; i++ {
		column := record.GetColumn(i)
		if column == nil {
			values[i] = ""
		} else {
			values[i] = task.columnToString(column)
		}
	}

	return strings.Join(values, task.writerJob.fieldDelimiter) + "\n"
}

func (task *StreamWriterTask) columnToString(column element.Column) string {
	if column == nil || column.IsNull() {
		return ""
	}

	switch column.GetType() {
	case element.TypeLong:
		if val, err := column.GetAsLong(); err == nil {
			return fmt.Sprintf("%d", val)
		}
	case element.TypeDouble:
		if val, err := column.GetAsDouble(); err == nil {
			return fmt.Sprintf("%.6f", val)
		}
	case element.TypeBool:
		if val, err := column.GetAsBool(); err == nil {
			return fmt.Sprintf("%t", val)
		}
	case element.TypeDate:
		if val, err := column.GetAsDate(); err == nil {
			return val.Format("2006-01-02 15:04:05")
		}
	case element.TypeBytes:
		if val, err := column.GetAsBytes(); err == nil {
			return fmt.Sprintf("%x", val) // 十六进制表示
		}
	case element.TypeString:
		return column.GetAsString()
	}

	return column.GetAsString()
}

func (task *StreamWriterTask) Post() error {
	return nil
}

func (task *StreamWriterTask) Destroy() error {
	if task.writer != nil {
		task.writer.Flush()
	}
	if task.file != nil {
		task.file.Close()
	}
	return nil
}