package jsonfilewriter

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
	coreplugin "github.com/longkeyy/go-datax/core/registry"
)

// JSONFileWriterJob JSON文件写入作业
type JSONFileWriterJob struct {
	config       config.Configuration
	path         string
	fileName     string
	writeMode    string
	encoding     string
	compress     string
	format       string // json 或 jsonl，默认 jsonl
	columns      []map[string]interface{}
	dateFormat   string
	nullFormat   string
	filePrefix   string
	fileSuffix   string
	maxFileSize  int64 // 单个文件最大大小（字节）
	maxRecords   int64 // 单个文件最大记录数
	factory      *factory.DataXFactory
}

func NewJSONFileWriterJob() *JSONFileWriterJob {
	return &JSONFileWriterJob{
		writeMode:   "truncate",
		encoding:    "UTF-8",
		format:      "jsonl", // 默认使用JSONL格式
		dateFormat:  time.RFC3339,
		nullFormat:  "",
		maxFileSize: 1024 * 1024 * 1024, // 默认1GB
		maxRecords:  1000000,             // 默认100万条记录
		factory:     factory.GetGlobalFactory(),
	}
}

func (job *JSONFileWriterJob) Init(config config.Configuration) error {
	job.config = config

	// 获取必需参数
	job.path = config.GetString("parameter.path")
	if job.path == "" {
		return fmt.Errorf("path is required")
	}

	// 获取文件名（可选）
	job.fileName = config.GetString("parameter.fileName")
	if job.fileName == "" {
		// 如果没有指定文件名，根据格式生成默认文件名
		job.fileName = "output"
	}

	// 获取列配置
	columnsConfig := config.GetList("parameter.column")
	if len(columnsConfig) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	for _, columnConfig := range columnsConfig {
		if column, ok := columnConfig.(map[string]interface{}); ok {
			job.columns = append(job.columns, column)
		}
	}

	if len(job.columns) == 0 {
		return fmt.Errorf("no valid columns configured")
	}

	// 获取可选参数
	job.writeMode = config.GetStringWithDefault("parameter.writeMode", "truncate")
	job.encoding = config.GetStringWithDefault("parameter.encoding", "UTF-8")
	job.compress = config.GetString("parameter.compress")
	job.format = config.GetStringWithDefault("parameter.format", "jsonl")
	job.dateFormat = config.GetStringWithDefault("parameter.dateFormat", time.RFC3339)
	job.nullFormat = config.GetStringWithDefault("parameter.nullFormat", "")
	job.filePrefix = config.GetString("parameter.filePrefix")
	job.fileSuffix = config.GetString("parameter.fileSuffix")

	// 文件大小和记录数限制
	job.maxFileSize = int64(config.GetIntWithDefault("parameter.maxFileSize", 1024*1024*1024)) // 1GB
	job.maxRecords = int64(config.GetIntWithDefault("parameter.maxRecords", 1000000))          // 100万条

	// 验证格式
	switch strings.ToLower(job.format) {
	case "json", "jsonl", "jsonlines":
		// 标准化格式名称
		if job.format == "jsonlines" {
			job.format = "jsonl"
		}
	default:
		log.Printf("Unknown format specified, using jsonl: %s", job.format)
		job.format = "jsonl"
	}

	// 确保输出目录存在
	if err := os.MkdirAll(job.path, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	log.Printf("JSONFileWriter initialized: path=%s, fileName=%s, format=%s, writeMode=%s, columns=%d",
		job.path, job.fileName, job.format, job.writeMode, len(job.columns))

	return nil
}

func (job *JSONFileWriterJob) Prepare() error {

	// 根据写入模式处理已存在的文件
	if job.writeMode == "truncate" {
		// 删除匹配的文件
		pattern := job.getFilePattern()
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return fmt.Errorf("failed to glob pattern %s: %v", pattern, err)
		}

		for _, match := range matches {
			if err := os.Remove(match); err != nil {
				log.Printf("Warning: failed to remove existing file %s: %v", match, err)
			} else {
				log.Printf("Removed existing file: %s", match)
			}
		}
	}

	return nil
}

func (job *JSONFileWriterJob) getFilePattern() string {
	var extension string
	switch job.format {
	case "json":
		extension = ".json"
	case "jsonl":
		extension = ".jsonl"
	default:
		extension = ".json"
	}

	if job.compress == "gzip" {
		extension += ".gz"
	}

	// 构建文件模式
	fileName := job.fileName
	if job.filePrefix != "" {
		fileName = job.filePrefix + fileName
	}
	if job.fileSuffix != "" {
		fileName = fileName + job.fileSuffix
	}

	return filepath.Join(job.path, fileName+"*"+extension)
}

func (job *JSONFileWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	taskConfigs := make([]config.Configuration, mandatoryNumber)

	for i := 0; i < mandatoryNumber; i++ {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfigs[i] = taskConfig
	}

	log.Printf("Split writer tasks: %d", mandatoryNumber)
	return taskConfigs, nil
}

func (job *JSONFileWriterJob) Post() error {
	return nil
}

func (job *JSONFileWriterJob) Destroy() error {
	return nil
}

// JSONFileWriterTask JSON文件写入任务
type JSONFileWriterTask struct {
	config        config.Configuration
	writerJob     *JSONFileWriterJob
	taskId        int
	currentFile   *os.File
	currentWriter io.Writer
	gzipWriter    *gzip.Writer
	recordCount   int64
	fileCount     int
	isFirstRecord bool
	jsonArray     []map[string]interface{} // 用于JSON格式的缓存
	factory       *factory.DataXFactory
}

func NewJSONFileWriterTask() *JSONFileWriterTask {
	return &JSONFileWriterTask{
		isFirstRecord: true,
		factory:       factory.GetGlobalFactory(),
	}
}

func (task *JSONFileWriterTask) Init(config config.Configuration) error {
	task.config = config

	// 创建WriterJob来重用配置逻辑
	task.writerJob = NewJSONFileWriterJob()
	err := task.writerJob.Init(config)
	if err != nil {
		return err
	}

	// 获取任务ID
	task.taskId = config.GetIntWithDefault("taskId", 0)

	log.Printf("JSONFileWriter task initialized: taskId=%d", task.taskId)
	return nil
}

func (task *JSONFileWriterTask) Prepare() error {
	return nil
}

func (task *JSONFileWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	totalRecords := int64(0)

	defer func() {
		// 确保文件正确关闭
		task.closeCurrentFile()
		log.Printf("Write task completed: totalRecords=%d, filesCreated=%d", totalRecords, task.fileCount)
	}()

	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == coreplugin.ErrChannelClosed {
				break
			}
			return fmt.Errorf("failed to receive record: %v", err)
		}

		// 检查是否需要创建新文件
		if task.needNewFile() {
			err = task.createNewFile()
			if err != nil {
				return fmt.Errorf("failed to create new file: %v", err)
			}
		}

		// 写入记录
		err = task.writeRecord(record)
		if err != nil {
			return fmt.Errorf("failed to write record: %v", err)
		}

		task.recordCount++
		totalRecords++

		// 每1000条记录输出一次进度
		if totalRecords%1000 == 0 {
			log.Printf("Writing progress: records=%d, currentFile=%d", totalRecords, task.fileCount)
		}
	}

	return nil
}

func (task *JSONFileWriterTask) needNewFile() bool {
	// 如果还没有文件，需要创建
	if task.currentFile == nil {
		return true
	}

	// 检查记录数限制
	if task.recordCount >= task.writerJob.maxRecords {
		return true
	}

	// 检查文件大小限制
	if stat, err := task.currentFile.Stat(); err == nil {
		if stat.Size() >= task.writerJob.maxFileSize {
			return true
		}
	}

	return false
}

func (task *JSONFileWriterTask) createNewFile() error {
	// 关闭当前文件
	err := task.closeCurrentFile()
	if err != nil {
		return err
	}

	// 生成新文件名
	fileName := task.generateFileName()
	filePath := filepath.Join(task.writerJob.path, fileName)

	// 创建新文件
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %v", filePath, err)
	}

	task.currentFile = file
	task.currentWriter = file

	// 如果启用了压缩
	if task.writerJob.compress == "gzip" {
		task.gzipWriter = gzip.NewWriter(file)
		task.currentWriter = task.gzipWriter
	}

	task.recordCount = 0
	task.fileCount++
	task.isFirstRecord = true

	// 如果是JSON格式，初始化数组缓存
	if task.writerJob.format == "json" {
		task.jsonArray = make([]map[string]interface{}, 0)
	}

	log.Printf("Created new file: %s (fileNumber=%d)", filePath, task.fileCount)

	return nil
}

func (task *JSONFileWriterTask) generateFileName() string {
	var extension string
	switch task.writerJob.format {
	case "json":
		extension = ".json"
	case "jsonl":
		extension = ".jsonl"
	default:
		extension = ".json"
	}

	if task.writerJob.compress == "gzip" {
		extension += ".gz"
	}

	// 构建文件名
	fileName := task.writerJob.fileName
	if task.writerJob.filePrefix != "" {
		fileName = task.writerJob.filePrefix + fileName
	}

	// 添加任务ID和文件序号
	fileName = fmt.Sprintf("%s_task%d_file%d", fileName, task.taskId, task.fileCount+1)

	if task.writerJob.fileSuffix != "" {
		fileName = fileName + task.writerJob.fileSuffix
	}

	return fileName + extension
}

func (task *JSONFileWriterTask) writeRecord(record element.Record) error {
	// 将记录转换为JSON对象
	jsonObj, err := task.convertRecordToJSON(record)
	if err != nil {
		return err
	}

	switch task.writerJob.format {
	case "json":
		// JSON格式：缓存到数组中，最后一次性写入
		task.jsonArray = append(task.jsonArray, jsonObj)
	case "jsonl":
		// JSONL格式：每行写入一个JSON对象
		jsonBytes, err := json.Marshal(jsonObj)
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %v", err)
		}

		_, err = task.currentWriter.Write(jsonBytes)
		if err != nil {
			return fmt.Errorf("failed to write JSON line: %v", err)
		}

		_, err = task.currentWriter.Write([]byte("\n"))
		if err != nil {
			return fmt.Errorf("failed to write newline: %v", err)
		}
	}

	return nil
}

func (task *JSONFileWriterTask) convertRecordToJSON(record element.Record) (map[string]interface{}, error) {
	jsonObj := make(map[string]interface{})

	columnCount := record.GetColumnNumber()
	for i := 0; i < columnCount; i++ {
		if i >= len(task.writerJob.columns) {
			break // 超出配置的列数
		}

		column := record.GetColumn(i)
		columnConfig := task.writerJob.columns[i]
		fieldName := task.getFieldName(columnConfig, i)
		value := task.convertColumnValue(column, columnConfig)

		jsonObj[fieldName] = value
	}

	return jsonObj, nil
}

func (task *JSONFileWriterTask) getFieldName(columnConfig map[string]interface{}, index int) string {
	// 优先使用name字段
	if name, ok := columnConfig["name"].(string); ok && name != "" {
		return name
	}

	// 如果没有name，使用index字段
	if indexVal, ok := columnConfig["index"]; ok {
		return fmt.Sprintf("field_%v", indexVal)
	}

	// 默认使用列索引
	return fmt.Sprintf("column_%d", index)
}

func (task *JSONFileWriterTask) convertColumnValue(column element.Column, columnConfig map[string]interface{}) interface{} {
	if column.IsNull() {
		if task.writerJob.nullFormat == "" {
			return nil
		}
		return task.writerJob.nullFormat
	}

	columnType, _ := columnConfig["type"].(string)

	switch columnType {
	case "long":
		if longVal, err := column.GetAsLong(); err == nil {
			return longVal
		}
		return int64(0)

	case "double":
		if doubleVal, err := column.GetAsDouble(); err == nil {
			return doubleVal
		}
		return float64(0)

	case "boolean":
		if boolVal, err := column.GetAsBool(); err == nil {
			return boolVal
		}
		return false

	case "date":
		if dateVal, err := column.GetAsDate(); err == nil {
			return dateVal.Format(task.writerJob.dateFormat)
		}
		return column.GetAsString()

	default: // string 或其他
		return column.GetAsString()
	}
}

func (task *JSONFileWriterTask) closeCurrentFile() error {
	if task.currentFile == nil {
		return nil
	}

	var err error

	// 如果是JSON格式，需要写入完整的数组
	if task.writerJob.format == "json" && len(task.jsonArray) > 0 {
		jsonBytes, marshallErr := json.MarshalIndent(task.jsonArray, "", "  ")
		if marshallErr != nil {
			return fmt.Errorf("failed to marshal JSON array: %v", marshallErr)
		}

		_, writeErr := task.currentWriter.Write(jsonBytes)
		if writeErr != nil {
			return fmt.Errorf("failed to write JSON array: %v", writeErr)
		}

		task.jsonArray = nil
	}

	// 关闭gzip writer
	if task.gzipWriter != nil {
		err = task.gzipWriter.Close()
		task.gzipWriter = nil
		if err != nil {
			return fmt.Errorf("failed to close gzip writer: %v", err)
		}
	}

	// 关闭文件
	if task.currentFile != nil {
		closeErr := task.currentFile.Close()
		task.currentFile = nil
		task.currentWriter = nil
		if closeErr != nil {
			return fmt.Errorf("failed to close file: %v", closeErr)
		}
	}

	return err
}

func (task *JSONFileWriterTask) Post() error {
	return task.closeCurrentFile()
}

func (task *JSONFileWriterTask) Destroy() error {
	return task.closeCurrentFile()
}