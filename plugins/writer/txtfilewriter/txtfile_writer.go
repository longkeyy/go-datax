package txtfilewriter

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
	coreplugin "github.com/longkeyy/go-datax/core/registry"
)

// TxtFileWriterJob 文本文件写入作业
type TxtFileWriterJob struct {
	config         config.Configuration
	path           string
	fileName       string
	writeMode      string
	fieldDelimiter string
	encoding       string
	nullFormat     string
	dateFormat     string
	fileFormat     string
	header         []string
	compress       string
	suffix         string
	factory        *factory.DataXFactory
}

func NewTxtFileWriterJob() *TxtFileWriterJob {
	return &TxtFileWriterJob{
		fieldDelimiter: ",",
		encoding:       "UTF-8",
		nullFormat:     "\\N",
		fileFormat:     "text",
		factory:        factory.GetGlobalFactory(),
	}
}

func (job *TxtFileWriterJob) Init(config config.Configuration) error {
	job.config = config

	// 获取必需参数
	job.path = config.GetString("parameter.path")
	if job.path == "" {
		return fmt.Errorf("path is required")
	}

	job.fileName = config.GetString("parameter.fileName")
	if job.fileName == "" {
		return fmt.Errorf("fileName is required")
	}

	job.writeMode = config.GetString("parameter.writeMode")
	if job.writeMode == "" {
		return fmt.Errorf("writeMode is required")
	}

	// 验证writeMode
	if job.writeMode != "truncate" && job.writeMode != "append" && job.writeMode != "nonConflict" {
		return fmt.Errorf("writeMode must be one of: truncate, append, nonConflict")
	}

	// 获取可选参数
	job.fieldDelimiter = config.GetStringWithDefault("parameter.fieldDelimiter", ",")
	job.encoding = config.GetStringWithDefault("parameter.encoding", "UTF-8")
	job.nullFormat = config.GetStringWithDefault("parameter.nullFormat", "\\N")
	job.dateFormat = config.GetString("parameter.dateFormat")
	job.fileFormat = config.GetStringWithDefault("parameter.fileFormat", "text")
	job.compress = config.GetString("parameter.compress")
	job.suffix = config.GetString("parameter.suffix")

	// 获取header配置
	headerList := config.GetList("parameter.header")
	for _, h := range headerList {
		if headerStr, ok := h.(string); ok {
			job.header = append(job.header, headerStr)
		}
	}

	log.Printf("TxtFileWriter initialized: path=%s, fileName=%s, writeMode=%s",
		job.path, job.fileName, job.writeMode)
	return nil
}

func (job *TxtFileWriterJob) Prepare() error {
	// 检查并创建目录
	if err := os.MkdirAll(job.path, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", job.path, err)
	}

	// 根据writeMode处理现有文件
	switch job.writeMode {
	case "truncate":
		// 删除所有以fileName为前缀的文件
		pattern := filepath.Join(job.path, job.fileName+"*")
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return fmt.Errorf("failed to glob files: %v", err)
		}
		for _, match := range matches {
			if err := os.Remove(match); err != nil {
				log.Printf("Warning: failed to remove file %s: %v", match, err)
			}
		}
	case "nonConflict":
		// 检查是否存在冲突文件
		pattern := filepath.Join(job.path, job.fileName+"*")
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return fmt.Errorf("failed to glob files: %v", err)
		}
		if len(matches) > 0 {
			return fmt.Errorf("files with prefix %s already exist in directory %s", job.fileName, job.path)
		}
	case "append":
		// append模式不需要特殊处理
	}

	return nil
}

func (job *TxtFileWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	taskConfigs := make([]config.Configuration, 0)

	// 创建指定数量的任务配置
	for i := 0; i < mandatoryNumber; i++ {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	log.Printf("Split into %d tasks", len(taskConfigs))
	return taskConfigs, nil
}

func (job *TxtFileWriterJob) Post() error {
	return nil
}

func (job *TxtFileWriterJob) Destroy() error {
	return nil
}

// TxtFileWriterTask 文本文件写入任务
type TxtFileWriterTask struct {
	config      config.Configuration
	writerJob   *TxtFileWriterJob
	outputFile  *os.File
	writer      io.Writer
	csvWriter   *csv.Writer
	recordCount int64
	taskId      int
	factory     *factory.DataXFactory
}

func NewTxtFileWriterTask() *TxtFileWriterTask {
	return &TxtFileWriterTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *TxtFileWriterTask) Init(config config.Configuration) error {
	task.config = config

	// 创建WriterJob来重用配置逻辑
	task.writerJob = NewTxtFileWriterJob()
	err := task.writerJob.Init(config)
	if err != nil {
		return err
	}

	// 获取任务ID
	task.taskId = config.GetIntWithDefault("taskId", 0)

	// 生成输出文件名
	fileName := task.generateFileName()
	filePath := filepath.Join(task.writerJob.path, fileName)

	// 创建输出文件
	var fileFlag int
	if task.writerJob.writeMode == "append" {
		fileFlag = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	} else {
		fileFlag = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	}

	task.outputFile, err = os.OpenFile(filePath, fileFlag, 0644)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %v", filePath, err)
	}

	// 创建写入器（支持压缩）
	task.writer = task.outputFile
	switch task.writerJob.compress {
	case "gzip":
		gzipWriter := gzip.NewWriter(task.outputFile)
		task.writer = gzipWriter
	case "bzip2":
		return fmt.Errorf("bzip2 compression not supported in this implementation")
	case "zip":
		return fmt.Errorf("zip compression not supported in this implementation")
	}

	// 根据fileFormat创建相应的写入器
	if task.writerJob.fileFormat == "csv" {
		task.csvWriter = csv.NewWriter(task.writer)
		// 设置分隔符
		if task.writerJob.fieldDelimiter == "\\t" {
			task.csvWriter.Comma = '\t'
		} else if len(task.writerJob.fieldDelimiter) > 0 {
			task.csvWriter.Comma = rune(task.writerJob.fieldDelimiter[0])
		}
	}

	// 写入表头
	if len(task.writerJob.header) > 0 && task.writerJob.writeMode != "append" {
		err = task.writeHeader()
		if err != nil {
			return fmt.Errorf("failed to write header: %v", err)
		}
	}

	log.Printf("TxtFileWriter task initialized: output file=%s", filePath)
	return nil
}

func (task *TxtFileWriterTask) generateFileName() string {
	// 生成随机后缀
	timestamp := time.Now().UnixNano()
	randomSuffix := fmt.Sprintf("_%d_%d", task.taskId, timestamp)

	fileName := task.writerJob.fileName + randomSuffix
	if task.writerJob.suffix != "" {
		fileName += task.writerJob.suffix
	}

	return fileName
}

func (task *TxtFileWriterTask) writeHeader() error {
	if task.writerJob.fileFormat == "csv" && task.csvWriter != nil {
		return task.csvWriter.Write(task.writerJob.header)
	} else {
		// text格式
		headerLine := strings.Join(task.writerJob.header, task.writerJob.fieldDelimiter) + "\n"
		_, err := task.writer.Write([]byte(headerLine))
		return err
	}
}

func (task *TxtFileWriterTask) Prepare() error {
	return nil
}

func (task *TxtFileWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	defer func() {
		// 确保数据被刷新和文件被关闭
		task.closeWriter()
	}()

	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == coreplugin.ErrChannelClosed {
				break
			}
			return fmt.Errorf("failed to receive record: %v", err)
		}

		// 写入记录
		err = task.writeRecord(record)
		if err != nil {
			return fmt.Errorf("failed to write record: %v", err)
		}

		task.recordCount++

		// 每1000条记录输出一次进度并刷新
		if task.recordCount%1000 == 0 {
			task.flushWriter()
			log.Printf("Written %d records", task.recordCount)
		}
	}

	log.Printf("Total records written: %d", task.recordCount)
	return nil
}

func (task *TxtFileWriterTask) writeRecord(record element.Record) error {
	columnCount := record.GetColumnNumber()
	if columnCount == 0 {
		return nil
	}

	if task.writerJob.fileFormat == "csv" && task.csvWriter != nil {
		// CSV格式写入
		row := make([]string, columnCount)
		for i := 0; i < columnCount; i++ {
			column := record.GetColumn(i)
			row[i] = task.formatColumnValue(column)
		}
		return task.csvWriter.Write(row)
	} else {
		// 文本格式写入
		values := make([]string, columnCount)
		for i := 0; i < columnCount; i++ {
			column := record.GetColumn(i)
			values[i] = task.formatColumnValue(column)
		}
		line := strings.Join(values, task.writerJob.fieldDelimiter) + "\n"
		_, err := task.writer.Write([]byte(line))
		return err
	}
}

func (task *TxtFileWriterTask) formatColumnValue(column element.Column) string {
	if column == nil || column.IsNull() {
		return task.writerJob.nullFormat
	}

	switch column.GetType() {
	case element.TypeLong:
		if val, err := column.GetAsLong(); err == nil {
			return strconv.FormatInt(val, 10)
		}
	case element.TypeDouble:
		if val, err := column.GetAsDouble(); err == nil {
			return strconv.FormatFloat(val, 'f', -1, 64)
		}
	case element.TypeBool:
		if val, err := column.GetAsBool(); err == nil {
			return strconv.FormatBool(val)
		}
	case element.TypeDate:
		if val, err := column.GetAsDate(); err == nil {
			if task.writerJob.dateFormat != "" {
				return val.Format(task.writerJob.dateFormat)
			}
			return val.Format("2006-01-02 15:04:05")
		}
	case element.TypeBytes:
		if val, err := column.GetAsBytes(); err == nil {
			return fmt.Sprintf("%x", val) // 十六进制表示
		}
	}

	return column.GetAsString()
}

func (task *TxtFileWriterTask) flushWriter() {
	if task.csvWriter != nil {
		task.csvWriter.Flush()
	}
	if gzipWriter, ok := task.writer.(*gzip.Writer); ok {
		gzipWriter.Flush()
	}
	if bufferedWriter, ok := task.writer.(*bufio.Writer); ok {
		bufferedWriter.Flush()
	}
}

func (task *TxtFileWriterTask) closeWriter() {
	if task.csvWriter != nil {
		task.csvWriter.Flush()
	}

	if gzipWriter, ok := task.writer.(*gzip.Writer); ok {
		gzipWriter.Close()
	}

	if task.outputFile != nil {
		task.outputFile.Close()
	}
}

func (task *TxtFileWriterTask) Post() error {
	return nil
}

func (task *TxtFileWriterTask) Destroy() error {
	task.closeWriter()
	return nil
}