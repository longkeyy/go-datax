package txtfilereader

import (
	"compress/bzip2"
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
)

// TxtFileReaderJob 文本文件读取作业
type TxtFileReaderJob struct {
	config           *config.Configuration
	paths            []string
	columns          []map[string]interface{}
	fieldDelimiter   string
	encoding         string
	compress         string
	skipHeader       bool
	nullFormat       string
	csvReaderConfig  map[string]interface{}
	resolvedFilePaths []string
}

func NewTxtFileReaderJob() *TxtFileReaderJob {
	return &TxtFileReaderJob{
		fieldDelimiter: ",",
		encoding:       "UTF-8",
		skipHeader:     false,
		nullFormat:     "\\N",
		csvReaderConfig: map[string]interface{}{
			"safetySwitch":     false,
			"skipEmptyRecords": false,
			"useTextQualifier": false,
		},
	}
}

func (job *TxtFileReaderJob) Init(config *config.Configuration) error {
	job.config = config

	// 获取必需参数
	pathsConfig := config.GetList("parameter.path")
	if len(pathsConfig) == 0 {
		// 尝试获取单个路径字符串
		if pathStr := config.GetString("parameter.path"); pathStr != "" {
			job.paths = []string{pathStr}
		} else {
			return fmt.Errorf("path is required")
		}
	} else {
		for _, path := range pathsConfig {
			if pathStr, ok := path.(string); ok {
				job.paths = append(job.paths, pathStr)
			}
		}
	}

	if len(job.paths) == 0 {
		return fmt.Errorf("no valid paths configured")
	}

	// 获取列配置
	columnsConfig := config.GetList("parameter.column")
	if len(columnsConfig) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	// 检查是否为通配符配置
	if len(columnsConfig) == 1 {
		if columnStr, ok := columnsConfig[0].(string); ok && columnStr == "*" {
			// 通配符配置，所有列都作为string类型
			job.columns = []map[string]interface{}{
				{"type": "string", "wildcard": true},
			}
		}
	}

	if len(job.columns) == 0 {
		for _, columnConfig := range columnsConfig {
			if column, ok := columnConfig.(map[string]interface{}); ok {
				// 设置默认类型为string
				if _, exists := column["type"]; !exists {
					column["type"] = "string"
				}
				job.columns = append(job.columns, column)
			}
		}
	}

	if len(job.columns) == 0 {
		return fmt.Errorf("no valid columns configured")
	}

	// 获取可选参数
	job.fieldDelimiter = config.GetStringWithDefault("parameter.fieldDelimiter", ",")
	job.encoding = config.GetStringWithDefault("parameter.encoding", "UTF-8")
	job.compress = config.GetString("parameter.compress")
	job.skipHeader = config.GetBoolWithDefault("parameter.skipHeader", false)
	job.nullFormat = config.GetStringWithDefault("parameter.nullFormat", "\\N")

	// 获取csvReaderConfig
	if csvConfig := config.GetMap("parameter.csvReaderConfig"); csvConfig != nil {
		job.csvReaderConfig = csvConfig
	}

	// 解析文件路径（包括通配符）
	err := job.resolveFilePaths()
	if err != nil {
		return fmt.Errorf("failed to resolve file paths: %v", err)
	}

	log.Printf("TxtFileReader initialized: paths=%v, files=%d, columns=%d",
		job.paths, len(job.resolvedFilePaths), len(job.columns))
	return nil
}

func (job *TxtFileReaderJob) resolveFilePaths() error {
	job.resolvedFilePaths = make([]string, 0)

	for _, pathPattern := range job.paths {
		if strings.Contains(pathPattern, "*") {
			// 通配符路径
			matches, err := filepath.Glob(pathPattern)
			if err != nil {
				return fmt.Errorf("failed to glob pattern %s: %v", pathPattern, err)
			}
			for _, match := range matches {
				if info, err := os.Stat(match); err == nil && !info.IsDir() {
					job.resolvedFilePaths = append(job.resolvedFilePaths, match)
				}
			}
		} else {
			// 直接路径
			if info, err := os.Stat(pathPattern); err == nil {
				if info.IsDir() {
					return fmt.Errorf("path %s is a directory, expected file", pathPattern)
				}
				job.resolvedFilePaths = append(job.resolvedFilePaths, pathPattern)
			} else {
				return fmt.Errorf("file %s does not exist", pathPattern)
			}
		}
	}

	if len(job.resolvedFilePaths) == 0 {
		return fmt.Errorf("no files found matching the path patterns")
	}

	return nil
}

func (job *TxtFileReaderJob) Prepare() error {
	return nil
}

func (job *TxtFileReaderJob) Split(adviceNumber int) ([]*config.Configuration, error) {
	taskConfigs := make([]*config.Configuration, 0)

	// 按文件进行分片
	fileCount := len(job.resolvedFilePaths)
	if adviceNumber > fileCount {
		adviceNumber = fileCount
	}

	filesPerTask := fileCount / adviceNumber
	if filesPerTask == 0 {
		filesPerTask = 1
	}

	for i := 0; i < adviceNumber; i++ {
		start := i * filesPerTask
		end := start + filesPerTask
		if i == adviceNumber-1 {
			end = fileCount // 最后一个任务处理剩余所有文件
		}

		if start >= fileCount {
			break
		}

		taskFiles := job.resolvedFilePaths[start:end]
		if len(taskFiles) == 0 {
			continue
		}

		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfig.Set("taskFiles", taskFiles)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	log.Printf("Split into %d tasks", len(taskConfigs))
	return taskConfigs, nil
}

func (job *TxtFileReaderJob) Post() error {
	return nil
}

func (job *TxtFileReaderJob) Destroy() error {
	return nil
}

// TxtFileReaderTask 文本文件读取任务
type TxtFileReaderTask struct {
	config    *config.Configuration
	readerJob *TxtFileReaderJob
	taskFiles []string
}

func NewTxtFileReaderTask() *TxtFileReaderTask {
	return &TxtFileReaderTask{}
}

func (task *TxtFileReaderTask) Init(config *config.Configuration) error {
	task.config = config

	// 创建ReaderJob来重用配置逻辑
	task.readerJob = NewTxtFileReaderJob()
	err := task.readerJob.Init(config)
	if err != nil {
		return err
	}

	// 获取任务文件列表
	if taskFiles := config.GetList("taskFiles"); taskFiles != nil {
		for _, file := range taskFiles {
			if fileStr, ok := file.(string); ok {
				task.taskFiles = append(task.taskFiles, fileStr)
			}
		}
	} else {
		// 如果没有指定任务文件，使用所有文件
		task.taskFiles = task.readerJob.resolvedFilePaths
	}

	log.Printf("TxtFileReader task initialized with %d files", len(task.taskFiles))
	return nil
}

func (task *TxtFileReaderTask) Prepare() error {
	return nil
}

func (task *TxtFileReaderTask) StartRead(recordSender plugin.RecordSender) error {
	totalRecords := int64(0)

	for _, filePath := range task.taskFiles {
		records, err := task.readFile(filePath, recordSender)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %v", filePath, err)
		}
		totalRecords += records
		log.Printf("Read %d records from file %s", records, filePath)
	}

	log.Printf("Completed reading %d total records", totalRecords)
	return nil
}

func (task *TxtFileReaderTask) readFile(filePath string, recordSender plugin.RecordSender) (int64, error) {
	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// 创建读取器（支持压缩）
	var reader io.Reader = file
	switch task.readerJob.compress {
	case "gzip":
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			return 0, fmt.Errorf("failed to create gzip reader: %v", err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	case "bzip2":
		reader = bzip2.NewReader(file)
	case "zip":
		return 0, fmt.Errorf("zip compression not supported for single file reading")
	}

	// 创建CSV读取器
	csvReader := csv.NewReader(reader)

	// 设置分隔符
	if task.readerJob.fieldDelimiter == "\\t" {
		csvReader.Comma = '\t'
	} else if len(task.readerJob.fieldDelimiter) > 0 {
		csvReader.Comma = rune(task.readerJob.fieldDelimiter[0])
	}

	// 配置CSV读取器
	if skipEmpty, ok := task.readerJob.csvReaderConfig["skipEmptyRecords"].(bool); ok {
		// CSV读取器本身不支持skipEmptyRecords，需要在读取时处理
		_ = skipEmpty
	}

	// 配置引号处理
	if useTextQualifier, ok := task.readerJob.csvReaderConfig["useTextQualifier"].(bool); ok && !useTextQualifier {
		// 设置LazyQuotes为true以允许更宽松的引号处理
		csvReader.LazyQuotes = true
	}

	recordCount := int64(0)
	lineNumber := 0

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return recordCount, fmt.Errorf("failed to read CSV record at line %d: %v", lineNumber+1, err)
		}

		lineNumber++

		// 跳过表头
		if task.readerJob.skipHeader && lineNumber == 1 {
			continue
		}

		// 跳过空行
		if skipEmpty, ok := task.readerJob.csvReaderConfig["skipEmptyRecords"].(bool); ok && skipEmpty {
			if len(record) == 0 || (len(record) == 1 && strings.TrimSpace(record[0]) == "") {
				continue
			}
		}

		// 转换为DataX记录
		dataxRecord, err := task.convertRecord(record)
		if err != nil {
			return recordCount, fmt.Errorf("failed to convert record at line %d: %v", lineNumber, err)
		}

		// 发送记录
		if err := recordSender.SendRecord(dataxRecord); err != nil {
			return recordCount, fmt.Errorf("failed to send record: %v", err)
		}

		recordCount++

		// 每1000条记录输出一次进度
		if recordCount%1000 == 0 {
			log.Printf("Read %d records from %s", recordCount, filePath)
		}
	}

	return recordCount, nil
}

func (task *TxtFileReaderTask) convertRecord(csvRecord []string) (element.Record, error) {
	record := element.NewRecord()

	// 处理通配符模式
	if len(task.readerJob.columns) == 1 {
		if _, isWildcard := task.readerJob.columns[0]["wildcard"]; isWildcard {
			// 通配符模式：所有字段都作为string
			for _, value := range csvRecord {
				column := task.convertValue(value, "string", "")
				record.AddColumn(column)
			}
			return record, nil
		}
	}

	// 常规模式：按配置的列信息处理
	for _, columnConfig := range task.readerJob.columns {
		column, err := task.convertColumnValue(csvRecord, columnConfig)
		if err != nil {
			return nil, err
		}
		record.AddColumn(column)
	}

	return record, nil
}

func (task *TxtFileReaderTask) convertColumnValue(csvRecord []string, columnConfig map[string]interface{}) (element.Column, error) {
	columnType, _ := columnConfig["type"].(string)
	format, _ := columnConfig["format"].(string)

	// 检查是否为常量值
	if value, hasValue := columnConfig["value"]; hasValue {
		valueStr := fmt.Sprintf("%v", value)
		return task.convertValue(valueStr, columnType, format), nil
	}

	// 检查是否有index
	index, hasIndex := columnConfig["index"]
	if !hasIndex {
		return nil, fmt.Errorf("column configuration must have either 'index' or 'value'")
	}

	indexInt, ok := index.(int)
	if !ok {
		if indexFloat, ok := index.(float64); ok {
			indexInt = int(indexFloat)
		} else {
			return nil, fmt.Errorf("column index must be an integer")
		}
	}

	// 检查索引是否越界
	if indexInt < 0 || indexInt >= len(csvRecord) {
		// 超出范围的字段返回null
		return element.NewStringColumn(""), nil
	}

	value := csvRecord[indexInt]
	return task.convertValue(value, columnType, format), nil
}

func (task *TxtFileReaderTask) convertValue(value, columnType, format string) element.Column {
	// 检查是否为null值
	if value == task.readerJob.nullFormat || value == "" {
		return element.NewStringColumn("")
	}

	switch columnType {
	case "long":
		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			return element.NewLongColumn(intVal)
		}
		return element.NewLongColumn(0)

	case "double":
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return element.NewDoubleColumn(floatVal)
		}
		return element.NewDoubleColumn(0.0)

	case "boolean":
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return element.NewBoolColumn(boolVal)
		}
		return element.NewBoolColumn(false)

	case "date":
		if format != "" {
			if dateVal, err := time.Parse(format, value); err == nil {
				return element.NewDateColumn(dateVal)
			}
		}
		// 尝试常见的日期格式
		dateFormats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02",
			"2006/01/02 15:04:05",
			"2006/01/02",
			"01/02/2006 15:04:05",
			"01/02/2006",
		}
		for _, fmt := range dateFormats {
			if dateVal, err := time.Parse(fmt, value); err == nil {
				return element.NewDateColumn(dateVal)
			}
		}
		return element.NewStringColumn(value)

	default: // string
		return element.NewStringColumn(value)
	}
}

func (task *TxtFileReaderTask) Post() error {
	return nil
}

func (task *TxtFileReaderTask) Destroy() error {
	return nil
}