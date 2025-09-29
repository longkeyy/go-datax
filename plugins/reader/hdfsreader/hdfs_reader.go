package hdfsreader

import (
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
)

// 常量定义，与Java版本保持一致
const (
	// 支持的文件类型
	FileTypeText    = "TEXT"
	FileTypeCSV     = "CSV"
	FileTypeORC     = "ORC"
	FileTypeSeq     = "SEQ"
	FileTypeRC      = "RC"
	FileTypeParquet = "PARQUET"

	// 配置键
	KeyDefaultFS              = "defaultFS"
	KeyPath                   = "path"
	KeyFileType              = "fileType"
	KeyEncoding              = "encoding"
	KeyColumn                = "column"
	KeyHaveKerberos          = "haveKerberos"
	KeyKerberosKeytabFilePath = "kerberosKeytabFilePath"
	KeyKerberosPrincipal     = "kerberosPrincipal"
	KeyKerberosConfFilePath  = "kerberosConfFilePath"
	KeyHadoopConfig          = "hadoopConfig"
	KeyHdfsUsername          = "hdfsUsername"
	KeySkipEmptyOrcFile      = "skipEmptyOrcFile"
	KeyOrcFileEmptySize      = "orcFileEmptySize"
	KeyFieldDelimiter        = "fieldDelimiter"
	KeyCompress              = "compress"
	KeyNullFormat            = "nullFormat"
)

// HdfsReaderJob HDFS文件读取作业
type HdfsReaderJob struct {
	config              config.Configuration
	defaultFS          string
	paths              []string
	fileType           string
	encoding           string
	columns            []map[string]interface{}
	haveKerberos       bool
	kerberosKeytabFile string
	kerberosPrincipal  string
	kerberosConfFile   string
	hadoopConfig       map[string]interface{}
	hdfsUsername       string
	skipEmptyOrcFile   bool
	orcFileEmptySize   int
	fieldDelimiter     string
	compress           string
	nullFormat         string
	resolvedFilePaths  []string
	factory            *factory.DataXFactory
}

func NewHdfsReaderJob() *HdfsReaderJob {
	return &HdfsReaderJob{
		fileType:       FileTypeText,
		encoding:       "UTF-8",
		hdfsUsername:   "admin",
		fieldDelimiter: ",",
		nullFormat:     "\\N",
		factory:        factory.GetGlobalFactory(),
	}
}

func (job *HdfsReaderJob) Init(config config.Configuration) error {
	job.config = config

	// 获取必需参数
	job.defaultFS = config.GetString("parameter.defaultFS")
	if job.defaultFS == "" {
		return fmt.Errorf("defaultFS is required")
	}

	// 解析path参数
	if err := job.parsePaths(); err != nil {
		return err
	}

	// 获取文件类型
	job.fileType = strings.ToUpper(config.GetStringWithDefault("parameter.fileType", FileTypeText))
	if !job.isValidFileType(job.fileType) {
		return fmt.Errorf("unsupported fileType: %s, supported types: TEXT, CSV, ORC, SEQ, RC, PARQUET", job.fileType)
	}

	// 获取编码
	job.encoding = config.GetStringWithDefault("parameter.encoding", "UTF-8")

	// 解析列配置
	if err := job.parseColumns(); err != nil {
		return err
	}

	// 获取Kerberos配置
	job.haveKerberos = config.GetBoolWithDefault("parameter.haveKerberos", false)
	if job.haveKerberos {
		job.kerberosKeytabFile = config.GetString("parameter.kerberosKeytabFilePath")
		job.kerberosPrincipal = config.GetString("parameter.kerberosPrincipal")
		job.kerberosConfFile = config.GetString("parameter.kerberosConfFilePath")

		if job.kerberosKeytabFile == "" || job.kerberosPrincipal == "" {
			return fmt.Errorf("kerberosKeytabFilePath and kerberosPrincipal are required when haveKerberos is true")
		}
	}

	// 获取其他配置
	job.hdfsUsername = config.GetStringWithDefault("parameter.hdfsUsername", "admin")
	job.skipEmptyOrcFile = config.GetBoolWithDefault("parameter.skipEmptyOrcFile", false)
	job.orcFileEmptySize = config.GetIntWithDefault("parameter.orcFileEmptySize", 0)
	job.fieldDelimiter = config.GetStringWithDefault("parameter.fieldDelimiter", ",")
	job.compress = config.GetString("parameter.compress")
	job.nullFormat = config.GetStringWithDefault("parameter.nullFormat", "\\N")

	// 获取Hadoop配置
	if hadoopConfig := config.GetMap("parameter.hadoopConfig"); hadoopConfig != nil {
		job.hadoopConfig = hadoopConfig
	}

	log.Printf("HdfsReader initialized: defaultFS=%s, paths=%v, fileType=%s, columns=%d",
		job.defaultFS, job.paths, job.fileType, len(job.columns))

	return nil
}

func (job *HdfsReaderJob) parsePaths() error {
	pathsConfig := job.config.GetList("parameter.path")
	if len(pathsConfig) == 0 {
		// 尝试获取单个路径字符串
		if pathStr := job.config.GetString("parameter.path"); pathStr != "" {
			job.paths = []string{pathStr}
		} else {
			return fmt.Errorf("path is required")
		}
	} else {
		for _, path := range pathsConfig {
			if pathStr, ok := path.(string); ok {
				// 验证路径必须是绝对路径
				if !strings.HasPrefix(pathStr, "/") {
					return fmt.Errorf("path must be absolute path: %s", pathStr)
				}
				job.paths = append(job.paths, pathStr)
			}
		}
	}

	if len(job.paths) == 0 {
		return fmt.Errorf("no valid paths configured")
	}

	return nil
}

func (job *HdfsReaderJob) parseColumns() error {
	columnsConfig := job.config.GetList("parameter.column")
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
			return nil
		}
	}

	for _, columnConfig := range columnsConfig {
		if column, ok := columnConfig.(map[string]interface{}); ok {
			// 验证type是必需的
			columnType, hasType := column["type"]
			if !hasType {
				return fmt.Errorf("column type is required")
			}

			// 验证index或value至少有一个
			_, hasIndex := column["index"]
			_, hasValue := column["value"]
			if !hasIndex && !hasValue {
				return fmt.Errorf("column must have either 'index' or 'value'")
			}
			if hasIndex && hasValue {
				return fmt.Errorf("column cannot have both 'index' and 'value'")
			}

			// 设置默认类型为string
			if columnType == "" {
				column["type"] = "string"
			}

			job.columns = append(job.columns, column)
		} else {
			return fmt.Errorf("invalid column configuration")
		}
	}

	if len(job.columns) == 0 {
		return fmt.Errorf("no valid columns configured")
	}

	return nil
}

func (job *HdfsReaderJob) isValidFileType(fileType string) bool {
	validTypes := []string{FileTypeText, FileTypeCSV, FileTypeORC, FileTypeSeq, FileTypeRC, FileTypeParquet}
	for _, vt := range validTypes {
		if vt == fileType {
			return true
		}
	}
	return false
}

func (job *HdfsReaderJob) Prepare() error {
	// 连接HDFS并获取文件列表
	client, err := job.createHdfsClient()
	if err != nil {
		return fmt.Errorf("failed to create HDFS client: %v", err)
	}
	defer client.Close()

	// 解析文件路径
	if err := job.resolveFilePaths(client); err != nil {
		return err
	}

	log.Printf("Found %d files to read: %v", len(job.resolvedFilePaths), job.resolvedFilePaths)
	return nil
}

func (job *HdfsReaderJob) createHdfsClient() (*hdfs.Client, error) {
	// 解析namenode地址
	address := strings.TrimPrefix(job.defaultFS, "hdfs://")

	options := hdfs.ClientOptions{
		Addresses: []string{address},
		User:      job.hdfsUsername,
	}

	// TODO: 添加Kerberos认证支持
	if job.haveKerberos {
		log.Printf("Warning: Kerberos authentication not yet implemented")
	}

	return hdfs.NewClient(options)
}

func (job *HdfsReaderJob) resolveFilePaths(client *hdfs.Client) error {
	job.resolvedFilePaths = make([]string, 0)

	for _, pathPattern := range job.paths {
		if strings.Contains(pathPattern, "*") {
			// 通配符路径处理
			matches, err := job.expandWildcard(client, pathPattern)
			if err != nil {
				return fmt.Errorf("failed to expand wildcard pattern %s: %v", pathPattern, err)
			}
			job.resolvedFilePaths = append(job.resolvedFilePaths, matches...)
		} else {
			// 直接路径
			fileInfo, err := client.Stat(pathPattern)
			if err != nil {
				return fmt.Errorf("path %s does not exist: %v", pathPattern, err)
			}

			if fileInfo.IsDir() {
				// 目录：获取目录下所有文件
				files, err := job.listDirectoryFiles(client, pathPattern)
				if err != nil {
					return fmt.Errorf("failed to list directory %s: %v", pathPattern, err)
				}
				job.resolvedFilePaths = append(job.resolvedFilePaths, files...)
			} else {
				// 文件：直接添加
				job.resolvedFilePaths = append(job.resolvedFilePaths, pathPattern)
			}
		}
	}

	if len(job.resolvedFilePaths) == 0 {
		return fmt.Errorf("no files found matching the path patterns")
	}

	return nil
}

func (job *HdfsReaderJob) expandWildcard(client *hdfs.Client, pattern string) ([]string, error) {
	// 简单的通配符实现，这里可以根据需要扩展
	dir := filepath.Dir(pattern)

	files, err := client.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var matches []string
	for _, file := range files {
		if !file.IsDir() {
			fullPath := filepath.Join(dir, file.Name())
			// 简单的文件名匹配（可以根据需要扩展为更复杂的通配符匹配）
			matched, _ := filepath.Match(filepath.Base(pattern), file.Name())
			if matched {
				matches = append(matches, fullPath)
			}
		}
	}

	return matches, nil
}

func (job *HdfsReaderJob) listDirectoryFiles(client *hdfs.Client, dirPath string) ([]string, error) {
	files, err := client.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	var filePaths []string
	for _, file := range files {
		if !file.IsDir() {
			fullPath := filepath.Join(dirPath, file.Name())
			filePaths = append(filePaths, fullPath)
		}
	}

	return filePaths, nil
}

func (job *HdfsReaderJob) Split(adviceNumber int) ([]config.Configuration, error) {
	taskConfigs := make([]config.Configuration, 0)

	// 按文件进行分片 - 每个slice处理一个文件（与Java版本保持一致）
	fileCount := len(job.resolvedFilePaths)
	if fileCount == 0 {
		return nil, fmt.Errorf("no files found to split")
	}

	// Java版本注释：warn:每个slice拖且仅拖一个文件
	splitNumber := fileCount
	if adviceNumber < splitNumber {
		// 如果建议的分片数小于文件数，每个分片可能包含多个文件
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
			taskConfig.Set("sourceFiles", taskFiles)
			taskConfigs = append(taskConfigs, taskConfig)
		}
	} else {
		// 每个文件一个分片
		for i, filePath := range job.resolvedFilePaths {
			taskConfig := job.config.Clone()
			taskConfig.Set("taskId", i)
			taskConfig.Set("sourceFiles", []string{filePath})
			taskConfigs = append(taskConfigs, taskConfig)
		}
	}

	log.Printf("Split into %d tasks for %d files", len(taskConfigs), fileCount)
	return taskConfigs, nil
}

func (job *HdfsReaderJob) Post() error {
	return nil
}

func (job *HdfsReaderJob) Destroy() error {
	return nil
}

// HdfsReaderTask HDFS文件读取任务
type HdfsReaderTask struct {
	config      config.Configuration
	readerJob   *HdfsReaderJob
	sourceFiles []string
	factory     *factory.DataXFactory
}

func NewHdfsReaderTask() *HdfsReaderTask {
	return &HdfsReaderTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *HdfsReaderTask) Init(config config.Configuration) error {
	task.config = config

	// 创建ReaderJob来重用配置逻辑
	task.readerJob = NewHdfsReaderJob()
	err := task.readerJob.Init(config)
	if err != nil {
		return err
	}

	// 获取任务文件列表
	if sourceFiles := config.GetList("sourceFiles"); sourceFiles != nil {
		for _, file := range sourceFiles {
			if fileStr, ok := file.(string); ok {
				task.sourceFiles = append(task.sourceFiles, fileStr)
			}
		}
	} else {
		return fmt.Errorf("sourceFiles not provided for task")
	}

	log.Printf("HdfsReader task initialized with %d files", len(task.sourceFiles))
	return nil
}

func (task *HdfsReaderTask) Prepare() error {
	return nil
}

func (task *HdfsReaderTask) StartRead(recordSender plugin.RecordSender) error {
	client, err := task.readerJob.createHdfsClient()
	if err != nil {
		return fmt.Errorf("failed to create HDFS client: %v", err)
	}
	defer client.Close()

	totalRecords := int64(0)

	for _, filePath := range task.sourceFiles {
		log.Printf("Reading file: %s", filePath)

		records, err := task.readFile(client, filePath, recordSender)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %v", filePath, err)
		}

		totalRecords += records
		log.Printf("Read %d records from file %s", records, filePath)

		// 刷新记录发送器
		if err := recordSender.Flush(); err != nil {
			return fmt.Errorf("failed to flush records: %v", err)
		}
	}

	log.Printf("Completed reading %d total records", totalRecords)
	return nil
}

func (task *HdfsReaderTask) readFile(client *hdfs.Client, filePath string, recordSender plugin.RecordSender) (int64, error) {
	// 根据文件类型选择不同的读取方式
	switch task.readerJob.fileType {
	case FileTypeText, FileTypeCSV:
		return task.readTextFile(client, filePath, recordSender)
	case FileTypeORC:
		return 0, fmt.Errorf("ORC format not yet implemented")
	case FileTypeSeq:
		return 0, fmt.Errorf("Sequence file format not yet implemented")
	case FileTypeRC:
		return 0, fmt.Errorf("RC file format not yet implemented")
	case FileTypeParquet:
		return 0, fmt.Errorf("Parquet format not yet implemented")
	default:
		return 0, fmt.Errorf("unsupported file type: %s", task.readerJob.fileType)
	}
}

func (task *HdfsReaderTask) readTextFile(client *hdfs.Client, filePath string, recordSender plugin.RecordSender) (int64, error) {
	// 打开HDFS文件
	file, err := client.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// TODO: 根据需要实现TEXT/CSV文件的具体解析逻辑
	// 这里先返回基本实现，后续可以扩展支持压缩、分隔符等

	recordCount := int64(0)

	// 简单实现：每行作为一个记录（实际应该根据列配置解析）
	// 这里需要实现类似UnstructuredStorageReaderUtil.readFromStream的逻辑

	log.Printf("Text file reading for %s (implementation pending)", filePath)

	return recordCount, nil
}

func (task *HdfsReaderTask) convertRecord(fields []string) (element.Record, error) {
	record := task.factory.GetRecordFactory().CreateRecord()

	// 处理通配符模式
	if len(task.readerJob.columns) == 1 {
		if _, isWildcard := task.readerJob.columns[0]["wildcard"]; isWildcard {
			// 通配符模式：所有字段都作为string
			for _, value := range fields {
				column := task.convertValue(value, "string", "")
				record.AddColumn(column)
			}
			return record, nil
		}
	}

	// 常规模式：按配置的列信息处理
	for _, columnConfig := range task.readerJob.columns {
		column, err := task.convertColumnValue(fields, columnConfig)
		if err != nil {
			return nil, err
		}
		record.AddColumn(column)
	}

	return record, nil
}

func (task *HdfsReaderTask) convertColumnValue(fields []string, columnConfig map[string]interface{}) (element.Column, error) {
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
	if indexInt < 0 || indexInt >= len(fields) {
		// 超出范围的字段返回null
		return task.factory.GetColumnFactory().CreateStringColumn(""), nil
	}

	value := fields[indexInt]
	return task.convertValue(value, columnType, format), nil
}

func (task *HdfsReaderTask) convertValue(value, columnType, format string) element.Column {
	// 检查是否为null值
	if value == task.readerJob.nullFormat || value == "" {
		return task.factory.GetColumnFactory().CreateStringColumn("")
	}

	columnFactory := task.factory.GetColumnFactory()

	switch columnType {
	case "long":
		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			return columnFactory.CreateLongColumn(intVal)
		}
		return columnFactory.CreateLongColumn(0)

	case "double":
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return columnFactory.CreateDoubleColumn(floatVal)
		}
		return columnFactory.CreateDoubleColumn(0.0)

	case "boolean":
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return columnFactory.CreateBoolColumn(boolVal)
		}
		return columnFactory.CreateBoolColumn(false)

	case "date":
		if format != "" {
			if dateVal, err := time.Parse(format, value); err == nil {
				return columnFactory.CreateDateColumn(dateVal)
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
				return columnFactory.CreateDateColumn(dateVal)
			}
		}
		return columnFactory.CreateStringColumn(value)

	default: // string
		return columnFactory.CreateStringColumn(value)
	}
}

func (task *HdfsReaderTask) Post() error {
	return nil
}

func (task *HdfsReaderTask) Destroy() error {
	return nil
}