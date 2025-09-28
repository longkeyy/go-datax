package jsonfilereader

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/plugin"
	"go.uber.org/zap"
)

// JSONFormat JSON文件格式枚举
type JSONFormat string

const (
	FormatJSON  JSONFormat = "json"  // 标准JSON格式，整个文件是一个JSON数组
	FormatJSONL JSONFormat = "jsonl" // JSON Lines格式，每行一个JSON对象
	FormatAuto  JSONFormat = "auto"  // 自动检测格式
)

// JSONFileReaderJob JSON文件读取作业
type JSONFileReaderJob struct {
	config            *config.Configuration
	paths             []string
	columns           []map[string]interface{}
	encoding          string
	compress          string
	nullFormat        string
	format            JSONFormat
	filePattern       string
	resolvedFilePaths []string
}

func NewJSONFileReaderJob() *JSONFileReaderJob {
	return &JSONFileReaderJob{
		encoding:   "UTF-8",
		nullFormat: "",
		format:     FormatAuto,
	}
}

func (job *JSONFileReaderJob) Init(config *config.Configuration) error {
	job.config = config
	compLogger := logger.Component().WithComponent("JSONFileReaderJob")

	// 获取路径配置 - 支持glob模式
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

	// 获取文件匹配模式（可选）
	job.filePattern = config.GetStringWithDefault("parameter.filePattern", "*.json*")

	// 获取列配置
	columnsConfig := config.GetList("parameter.column")

	// 支持自动推断列配置
	autoInfer := config.GetBoolWithDefault("parameter.autoInferSchema", false)

	if len(columnsConfig) == 0 && !autoInfer {
		return fmt.Errorf("column configuration is required or enable autoInferSchema")
	}

	// 检查是否为通配符配置
	if len(columnsConfig) == 1 {
		if columnStr, ok := columnsConfig[0].(string); ok && columnStr == "*" {
			// 通配符配置，所有字段都作为string类型
			job.columns = []map[string]interface{}{
				{"type": "string", "wildcard": true},
			}
		}
	}

	if len(job.columns) == 0 && len(columnsConfig) > 0 {
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

	// 如果启用了自动推断，先解析文件路径，然后推断schema
	if autoInfer || len(job.columns) == 0 {
		// 需要先解析文件路径
		err := job.resolveFilePaths()
		if err != nil {
			return fmt.Errorf("failed to resolve file paths for schema inference: %v", err)
		}

		// 推断schema
		inferredColumns, err := job.inferSchema()
		if err != nil {
			return fmt.Errorf("failed to infer schema: %v", err)
		}

		if len(job.columns) == 0 {
			job.columns = inferredColumns
		} else {
			// 如果已有配置，与推断结果合并
			job.mergeInferredColumns(inferredColumns)
		}
	}

	// 获取可选参数
	job.encoding = config.GetStringWithDefault("parameter.encoding", "UTF-8")
	job.compress = config.GetString("parameter.compress")
	job.nullFormat = config.GetStringWithDefault("parameter.nullFormat", "")

	// 获取JSON格式配置
	formatStr := config.GetStringWithDefault("parameter.format", "auto")
	switch strings.ToLower(formatStr) {
	case "json":
		job.format = FormatJSON
	case "jsonl", "jsonlines":
		job.format = FormatJSONL
	case "auto":
		job.format = FormatAuto
	default:
		compLogger.Warn("Unknown format specified, using auto detection",
			zap.String("format", formatStr))
		job.format = FormatAuto
	}

	// 如果还没有解析文件路径，现在解析
	if len(job.resolvedFilePaths) == 0 {
		err := job.resolveFilePaths()
		if err != nil {
			return fmt.Errorf("failed to resolve file paths: %v", err)
		}
	}

	compLogger.Info("JSONFileReader initialized",
		zap.Strings("paths", job.paths),
		zap.Int("files", len(job.resolvedFilePaths)),
		zap.Int("columns", len(job.columns)),
		zap.String("format", string(job.format)))
	return nil
}

func (job *JSONFileReaderJob) resolveFilePaths() error {
	job.resolvedFilePaths = make([]string, 0)
	compLogger := logger.Component().WithComponent("JSONFileReaderJob")

	for _, pathPattern := range job.paths {
		compLogger.Debug("Processing path pattern", zap.String("pattern", pathPattern))

		if strings.Contains(pathPattern, "*") {
			// 通配符路径，支持递归模式如 /*/**.jsonl
			matches, err := filepath.Glob(pathPattern)
			if err != nil {
				return fmt.Errorf("failed to glob pattern %s: %v", pathPattern, err)
			}

			// 如果没有匹配到，尝试处理递归模式
			if len(matches) == 0 && strings.Contains(pathPattern, "/*") {
				err = job.walkDirectoryPattern(pathPattern)
				if err != nil {
					return err
				}
			} else {
				for _, match := range matches {
					if err := job.addFileIfValid(match); err != nil {
						compLogger.Warn("Skipping invalid file",
							zap.String("file", match),
							zap.Error(err))
					}
				}
			}
		} else {
			// 直接路径
			if info, err := os.Stat(pathPattern); err == nil {
				if info.IsDir() {
					// 如果是目录，则查找目录下的JSON文件
					err = job.walkDirectory(pathPattern)
					if err != nil {
						return err
					}
				} else {
					// 单个文件
					if err := job.addFileIfValid(pathPattern); err != nil {
						return err
					}
				}
			} else {
				return fmt.Errorf("path %s does not exist", pathPattern)
			}
		}
	}

	if len(job.resolvedFilePaths) == 0 {
		return fmt.Errorf("no JSON files found matching the path patterns")
	}

	compLogger.Info("File resolution completed",
		zap.Int("totalFiles", len(job.resolvedFilePaths)))
	return nil
}

func (job *JSONFileReaderJob) walkDirectoryPattern(pattern string) error {
	// 处理如 /*/**.jsonl 这样的递归模式
	parts := strings.Split(pattern, "/")
	var basePath string
	var searchPattern string

	// 找到第一个包含通配符的部分
	for i, part := range parts {
		if strings.Contains(part, "*") {
			if i > 0 {
				basePath = strings.Join(parts[:i], "/")
			} else {
				basePath = "."
			}
			searchPattern = strings.Join(parts[i:], "/")
			break
		}
	}

	return filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // 忽略错误，继续遍历
		}

		if info.IsDir() {
			return nil
		}

		// 检查文件是否匹配模式
		relPath, err := filepath.Rel(basePath, path)
		if err != nil {
			return nil
		}

		matched, err := filepath.Match(searchPattern, relPath)
		if err != nil {
			return nil
		}

		if matched {
			job.addFileIfValid(path)
		}

		return nil
	})
}

func (job *JSONFileReaderJob) walkDirectory(dirPath string) error {
	return filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // 忽略错误，继续遍历
		}

		if info.IsDir() {
			return nil
		}

		// 检查文件是否匹配JSON文件模式
		if job.isJSONFile(path) {
			job.addFileIfValid(path)
		}

		return nil
	})
}

func (job *JSONFileReaderJob) isJSONFile(filePath string) bool {
	ext := strings.ToLower(filepath.Ext(filePath))
	jsonExtensions := []string{".json", ".jsonl", ".ndjson", ".ldjson"}

	for _, jsonExt := range jsonExtensions {
		if ext == jsonExt {
			return true
		}
	}

	// 检查是否匹配自定义文件模式
	if job.filePattern != "" {
		matched, _ := filepath.Match(job.filePattern, filepath.Base(filePath))
		return matched
	}

	return false
}

func (job *JSONFileReaderJob) addFileIfValid(filePath string) error {
	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}

	if info.IsDir() {
		return fmt.Errorf("path %s is a directory, expected file", filePath)
	}

	if !job.isJSONFile(filePath) {
		return fmt.Errorf("file %s is not a JSON file", filePath)
	}

	job.resolvedFilePaths = append(job.resolvedFilePaths, filePath)
	return nil
}

func (job *JSONFileReaderJob) Prepare() error {
	return nil
}

func (job *JSONFileReaderJob) Split(adviceNumber int) ([]*config.Configuration, error) {
	taskConfigs := make([]*config.Configuration, 0)
	compLogger := logger.Component().WithComponent("JSONFileReaderJob")

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

	compLogger.Info("Split tasks completed", zap.Int("taskCount", len(taskConfigs)))
	return taskConfigs, nil
}

func (job *JSONFileReaderJob) Post() error {
	return nil
}

func (job *JSONFileReaderJob) Destroy() error {
	return nil
}

func (job *JSONFileReaderJob) detectJSONFormat(reader io.Reader) (JSONFormat, error) {
	// 读取文件前几个字节来判断格式
	buffer := make([]byte, 1024)
	n, err := reader.Read(buffer)
	if err != nil && err != io.EOF {
		return FormatAuto, err
	}

	content := strings.TrimSpace(string(buffer[:n]))

	// 如果以 [ 开头，很可能是JSON数组格式
	if strings.HasPrefix(content, "[") {
		return FormatJSON, nil
	}

	// 如果以 { 开头，很可能是JSONL格式
	if strings.HasPrefix(content, "{") {
		return FormatJSONL, nil
	}

	// 默认尝试JSONL格式
	return FormatJSONL, nil
}

// inferSchema 通过预读文件来推断JSON schema
func (job *JSONFileReaderJob) inferSchema() ([]map[string]interface{}, error) {
	compLogger := logger.Component().WithComponent("JSONFileReaderJob")

	if len(job.resolvedFilePaths) == 0 {
		return nil, fmt.Errorf("no files available for schema inference")
	}

	// 选择前几个文件进行推断，最多3个文件
	maxFiles := 3
	if len(job.resolvedFilePaths) < maxFiles {
		maxFiles = len(job.resolvedFilePaths)
	}

	fieldStats := make(map[string]*FieldInfo)
	totalSamples := 0
	maxSamples := 100 // 最多预读100行

	compLogger.Info("Starting schema inference",
		zap.Int("maxFiles", maxFiles),
		zap.Int("maxSamples", maxSamples))

	for i := 0; i < maxFiles && totalSamples < maxSamples; i++ {
		filePath := job.resolvedFilePaths[i]
		samples, err := job.sampleFile(filePath, maxSamples-totalSamples)
		if err != nil {
			compLogger.Warn("Failed to sample file, skipping",
				zap.String("file", filePath),
				zap.Error(err))
			continue
		}

		// 分析样本数据
		for _, sample := range samples {
			job.analyzeJSONObject(sample, fieldStats)
			totalSamples++
		}

		compLogger.Debug("File sampling completed",
			zap.String("file", filePath),
			zap.Int("samples", len(samples)))
	}

	if len(fieldStats) == 0 {
		return nil, fmt.Errorf("no fields found during schema inference")
	}

	// 将统计信息转换为列配置
	columns := make([]map[string]interface{}, 0, len(fieldStats))
	for fieldName, fieldInfo := range fieldStats {
		inferredType := job.inferFieldType(fieldInfo)
		column := map[string]interface{}{
			"name": fieldName,
			"type": inferredType,
		}
		columns = append(columns, column)
	}

	compLogger.Info("Schema inference completed",
		zap.Int("totalSamples", totalSamples),
		zap.Int("fields", len(columns)))

	return columns, nil
}

// FieldInfo 字段统计信息
type FieldInfo struct {
	Name        string
	TotalCount  int
	NullCount   int
	StringCount int
	IntCount    int
	FloatCount  int
	BoolCount   int
	ObjectCount int
	ArrayCount  int
	Examples    []interface{} // 保存一些示例值
}

// sampleFile 从文件中采样数据
func (job *JSONFileReaderJob) sampleFile(filePath string, maxSamples int) ([]map[string]interface{}, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 创建读取器（支持压缩）
	var reader io.Reader = file
	switch job.compress {
	case "gzip":
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, err
		}
		defer gzipReader.Close()
		reader = gzipReader
	case "bzip2":
		reader = bzip2.NewReader(file)
	}

	// 检测格式
	format := job.format
	if format == FormatAuto {
		format, err = job.detectJSONFormat(reader)
		if err != nil {
			return nil, err
		}

		// 重新打开文件
		file.Close()
		file, err = os.Open(filePath)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		reader = file
		if job.compress == "gzip" {
			gzipReader, err := gzip.NewReader(file)
			if err != nil {
				return nil, err
			}
			defer gzipReader.Close()
			reader = gzipReader
		} else if job.compress == "bzip2" {
			reader = bzip2.NewReader(file)
		}
	}

	var samples []map[string]interface{}

	switch format {
	case FormatJSON:
		samples, err = job.sampleJSONFile(reader, maxSamples)
	case FormatJSONL:
		samples, err = job.sampleJSONLFile(reader, maxSamples)
	default:
		return nil, fmt.Errorf("unsupported format for sampling: %s", format)
	}

	return samples, err
}

// sampleJSONFile 从JSON数组文件中采样
func (job *JSONFileReaderJob) sampleJSONFile(reader io.Reader, maxSamples int) ([]map[string]interface{}, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	var jsonArray []interface{}
	if err := json.Unmarshal(data, &jsonArray); err != nil {
		return nil, err
	}

	var samples []map[string]interface{}
	sampleCount := maxSamples
	if len(jsonArray) < sampleCount {
		sampleCount = len(jsonArray)
	}

	for i := 0; i < sampleCount; i++ {
		if objMap, ok := jsonArray[i].(map[string]interface{}); ok {
			samples = append(samples, objMap)
		}
	}

	return samples, nil
}

// sampleJSONLFile 从JSONL文件中采样
func (job *JSONFileReaderJob) sampleJSONLFile(reader io.Reader, maxSamples int) ([]map[string]interface{}, error) {
	scanner := bufio.NewScanner(reader)
	var samples []map[string]interface{}
	count := 0

	for scanner.Scan() && count < maxSamples {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var jsonObj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &jsonObj); err != nil {
			continue // 跳过解析失败的行
		}

		samples = append(samples, jsonObj)
		count++
	}

	return samples, scanner.Err()
}

// analyzeJSONObject 分析JSON对象的字段
func (job *JSONFileReaderJob) analyzeJSONObject(jsonObj map[string]interface{}, fieldStats map[string]*FieldInfo) {
	for fieldName, value := range jsonObj {
		if fieldStats[fieldName] == nil {
			fieldStats[fieldName] = &FieldInfo{
				Name:     fieldName,
				Examples: make([]interface{}, 0, 5),
			}
		}

		fieldInfo := fieldStats[fieldName]
		fieldInfo.TotalCount++

		// 保存示例值（最多5个）
		if len(fieldInfo.Examples) < 5 {
			fieldInfo.Examples = append(fieldInfo.Examples, value)
		}

		// 分析值类型
		job.analyzeValueType(value, fieldInfo)
	}
}

// analyzeValueType 分析值的类型
func (job *JSONFileReaderJob) analyzeValueType(value interface{}, fieldInfo *FieldInfo) {
	if value == nil {
		fieldInfo.NullCount++
		return
	}

	switch v := value.(type) {
	case string:
		fieldInfo.StringCount++
	case int, int64:
		fieldInfo.IntCount++
	case float64:
		// 检查是否实际上是整数
		if v == float64(int64(v)) {
			fieldInfo.IntCount++
		} else {
			fieldInfo.FloatCount++
		}
	case bool:
		fieldInfo.BoolCount++
	case map[string]interface{}:
		fieldInfo.ObjectCount++
	case []interface{}:
		fieldInfo.ArrayCount++
	default:
		fieldInfo.StringCount++ // 其他类型当作字符串处理
	}
}

// inferFieldType 根据统计信息推断字段类型
func (job *JSONFileReaderJob) inferFieldType(fieldInfo *FieldInfo) string {
	nonNullCount := fieldInfo.TotalCount - fieldInfo.NullCount
	if nonNullCount == 0 {
		return "string" // 全是null，默认string
	}

	// 计算各类型占比
	stringRatio := float64(fieldInfo.StringCount) / float64(nonNullCount)
	intRatio := float64(fieldInfo.IntCount) / float64(nonNullCount)
	floatRatio := float64(fieldInfo.FloatCount) / float64(nonNullCount)
	boolRatio := float64(fieldInfo.BoolCount) / float64(nonNullCount)

	// 如果某种类型占比超过80%，使用该类型
	threshold := 0.8

	if boolRatio >= threshold {
		return "boolean"
	}
	if intRatio >= threshold {
		return "long"
	}
	if floatRatio >= threshold || (intRatio+floatRatio) >= threshold {
		return "double"
	}

	// 尝试检测日期字符串
	if stringRatio >= threshold {
		if job.isLikelyDateField(fieldInfo) {
			return "date"
		}
		return "string"
	}

	// 默认返回string
	return "string"
}

// isLikelyDateField 检测是否可能是日期字段
func (job *JSONFileReaderJob) isLikelyDateField(fieldInfo *FieldInfo) bool {
	dateKeywords := []string{"date", "time", "created", "updated", "timestamp"}

	// 检查字段名是否包含日期关键词
	fieldNameLower := strings.ToLower(fieldInfo.Name)
	for _, keyword := range dateKeywords {
		if strings.Contains(fieldNameLower, keyword) {
			// 检查示例值是否像日期
			for _, example := range fieldInfo.Examples {
				if strVal, ok := example.(string); ok {
					if job.looksLikeDate(strVal) {
						return true
					}
				}
			}
		}
	}

	return false
}

// looksLikeDate 检查字符串是否看起来像日期
func (job *JSONFileReaderJob) looksLikeDate(str string) bool {
	datePatterns := []string{
		`\d{4}-\d{2}-\d{2}`,                    // 2023-01-01
		`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`, // 2023-01-01T12:00:00
		`\d{4}/\d{2}/\d{2}`,                    // 2023/01/01
		`\d{2}/\d{2}/\d{4}`,                    // 01/01/2023
	}

	for _, pattern := range datePatterns {
		// 简单的模式匹配，不使用正则表达式库
		if job.simpleMatch(str, pattern) {
			return true
		}
	}

	return false
}

// simpleMatch 简单的模式匹配
func (job *JSONFileReaderJob) simpleMatch(str, pattern string) bool {
	// 简化实现：检查一些常见的日期格式
	if len(str) >= 10 {
		// YYYY-MM-DD 格式
		if len(str) >= 10 && str[4] == '-' && str[7] == '-' {
			return true
		}
		// YYYY/MM/DD 格式
		if len(str) >= 10 && str[4] == '/' && str[7] == '/' {
			return true
		}
		// MM/DD/YYYY 格式
		if len(str) >= 10 && str[2] == '/' && str[5] == '/' {
			return true
		}
	}
	return false
}

// mergeInferredColumns 合并推断的列配置和用户配置
func (job *JSONFileReaderJob) mergeInferredColumns(inferredColumns []map[string]interface{}) {
	// 创建推断字段的映射
	inferredMap := make(map[string]map[string]interface{})
	for _, col := range inferredColumns {
		if name, ok := col["name"].(string); ok {
			inferredMap[name] = col
		}
	}

	// 更新现有配置的字段类型
	for _, col := range job.columns {
		if name, ok := col["name"].(string); ok {
			if inferredCol, exists := inferredMap[name]; exists {
				// 如果用户没有指定类型，使用推断的类型
				if _, hasType := col["type"]; !hasType {
					col["type"] = inferredCol["type"]
				}
			}
		}
	}
}

// JSONFileReaderTask JSON文件读取任务
type JSONFileReaderTask struct {
	config    *config.Configuration
	readerJob *JSONFileReaderJob
	taskFiles []string
}

func NewJSONFileReaderTask() *JSONFileReaderTask {
	return &JSONFileReaderTask{}
}

func (task *JSONFileReaderTask) Init(config *config.Configuration) error {
	task.config = config
	compLogger := logger.Component().WithComponent("JSONFileReaderTask")

	// 创建ReaderJob来重用配置逻辑
	task.readerJob = NewJSONFileReaderJob()
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

	compLogger.Info("JSONFileReader task initialized", zap.Int("fileCount", len(task.taskFiles)))
	return nil
}

func (task *JSONFileReaderTask) Prepare() error {
	return nil
}

func (task *JSONFileReaderTask) StartRead(recordSender plugin.RecordSender) error {
	totalRecords := int64(0)
	compLogger := logger.Component().WithComponent("JSONFileReaderTask")

	for _, filePath := range task.taskFiles {
		records, err := task.readFile(filePath, recordSender)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %v", filePath, err)
		}
		totalRecords += records
		compLogger.Info("File processing completed",
			zap.String("file", filePath),
			zap.Int64("records", records))
	}

	compLogger.Info("Read task completed", zap.Int64("totalRecords", totalRecords))
	return nil
}

func (task *JSONFileReaderTask) readFile(filePath string, recordSender plugin.RecordSender) (int64, error) {
	compLogger := logger.Component().WithComponent("JSONFileReaderTask")

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

	// 检测JSON格式
	format := task.readerJob.format
	if format == FormatAuto {
		format, err = task.detectJSONFormat(reader)
		if err != nil {
			return 0, fmt.Errorf("failed to detect JSON format: %v", err)
		}
		compLogger.Debug("Format detected",
			zap.String("file", filePath),
			zap.String("format", string(format)))

		// 重新打开文件，因为detectJSONFormat会消耗reader
		file.Close()
		file, err = os.Open(filePath)
		if err != nil {
			return 0, fmt.Errorf("failed to reopen file: %v", err)
		}
		defer file.Close()

		// 重新创建reader
		reader = file
		if task.readerJob.compress == "gzip" {
			gzipReader, err := gzip.NewReader(file)
			if err != nil {
				return 0, fmt.Errorf("failed to create gzip reader: %v", err)
			}
			defer gzipReader.Close()
			reader = gzipReader
		} else if task.readerJob.compress == "bzip2" {
			reader = bzip2.NewReader(file)
		}
	}

	// 根据格式读取
	switch format {
	case FormatJSON:
		return task.readJSONFile(reader, recordSender)
	case FormatJSONL:
		return task.readJSONLFile(reader, recordSender)
	default:
		return 0, fmt.Errorf("unsupported JSON format: %s", format)
	}
}

func (task *JSONFileReaderTask) detectJSONFormat(reader io.Reader) (JSONFormat, error) {
	// 读取文件前几个字节来判断格式
	buffer := make([]byte, 1024)
	n, err := reader.Read(buffer)
	if err != nil && err != io.EOF {
		return FormatAuto, err
	}

	content := strings.TrimSpace(string(buffer[:n]))

	// 如果以 [ 开头，很可能是JSON数组格式
	if strings.HasPrefix(content, "[") {
		return FormatJSON, nil
	}

	// 如果以 { 开头，很可能是JSONL格式
	if strings.HasPrefix(content, "{") {
		return FormatJSONL, nil
	}

	// 默认尝试JSONL格式
	return FormatJSONL, nil
}

func (task *JSONFileReaderTask) readJSONFile(reader io.Reader, recordSender plugin.RecordSender) (int64, error) {
	// 读取整个JSON文件
	data, err := io.ReadAll(reader)
	if err != nil {
		return 0, fmt.Errorf("failed to read JSON file: %v", err)
	}

	// 解析JSON数组
	var jsonArray []interface{}
	if err := json.Unmarshal(data, &jsonArray); err != nil {
		return 0, fmt.Errorf("failed to parse JSON array: %v", err)
	}

	recordCount := int64(0)
	for i, jsonObj := range jsonArray {
		record, err := task.convertJSONToRecord(jsonObj)
		if err != nil {
			return recordCount, fmt.Errorf("failed to convert JSON object at index %d: %v", i, err)
		}

		if err := recordSender.SendRecord(record); err != nil {
			return recordCount, fmt.Errorf("failed to send record: %v", err)
		}

		recordCount++

		// 每1000条记录输出一次进度
		if recordCount%1000 == 0 {
			logger.Component().WithComponent("JSONFileReaderTask").Debug("Reading progress",
				zap.Int64("records", recordCount))
		}
	}

	return recordCount, nil
}

func (task *JSONFileReaderTask) readJSONLFile(reader io.Reader, recordSender plugin.RecordSender) (int64, error) {
	scanner := bufio.NewScanner(reader)
	recordCount := int64(0)
	lineNumber := 0

	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())

		// 跳过空行
		if line == "" {
			continue
		}

		// 解析JSON对象
		var jsonObj interface{}
		if err := json.Unmarshal([]byte(line), &jsonObj); err != nil {
			return recordCount, fmt.Errorf("failed to parse JSON at line %d: %v", lineNumber, err)
		}

		record, err := task.convertJSONToRecord(jsonObj)
		if err != nil {
			return recordCount, fmt.Errorf("failed to convert JSON object at line %d: %v", lineNumber, err)
		}

		if err := recordSender.SendRecord(record); err != nil {
			return recordCount, fmt.Errorf("failed to send record: %v", err)
		}

		recordCount++

		// 每1000条记录输出一次进度
		if recordCount%1000 == 0 {
			logger.Component().WithComponent("JSONFileReaderTask").Debug("Reading progress",
				zap.Int64("records", recordCount))
		}
	}

	if err := scanner.Err(); err != nil {
		return recordCount, fmt.Errorf("failed to scan JSONL file: %v", err)
	}

	return recordCount, nil
}

func (task *JSONFileReaderTask) convertJSONToRecord(jsonObj interface{}) (element.Record, error) {
	record := element.NewRecord()

	// 将JSON对象转换为map
	var objMap map[string]interface{}
	switch v := jsonObj.(type) {
	case map[string]interface{}:
		objMap = v
	default:
		// 如果不是对象，尝试序列化再反序列化
		jsonBytes, err := json.Marshal(jsonObj)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal JSON object: %v", err)
		}
		if err := json.Unmarshal(jsonBytes, &objMap); err != nil {
			// 如果还是失败，直接作为字符串处理
			record.AddColumn(element.NewStringColumn(string(jsonBytes)))
			return record, nil
		}
	}

	// 处理通配符模式
	if len(task.readerJob.columns) == 1 {
		if _, isWildcard := task.readerJob.columns[0]["wildcard"]; isWildcard {
			// 通配符模式：按JSON对象的键值对顺序添加所有字段
			for _, value := range objMap {
				column := task.convertJSONValue(value, "string", "")
				record.AddColumn(column)
			}
			return record, nil
		}
	}

	// 常规模式：按配置的列信息处理
	for _, columnConfig := range task.readerJob.columns {
		column, err := task.convertJSONColumnValue(objMap, columnConfig)
		if err != nil {
			return nil, err
		}
		record.AddColumn(column)
	}

	return record, nil
}

func (task *JSONFileReaderTask) convertJSONColumnValue(jsonObj map[string]interface{}, columnConfig map[string]interface{}) (element.Column, error) {
	columnType, _ := columnConfig["type"].(string)
	format, _ := columnConfig["format"].(string)

	// 检查是否为常量值
	if value, hasValue := columnConfig["value"]; hasValue {
		return task.convertJSONValue(value, columnType, format), nil
	}

	// 检查是否有name字段（JSON对象的键名）
	fieldName, hasName := columnConfig["name"]
	if !hasName {
		// 如果没有name，检查是否有index（兼容性）
		if index, hasIndex := columnConfig["index"]; hasIndex {
			// 对于JSON，index不太适用，但可以尝试转换为字符串键
			fieldName = fmt.Sprintf("%v", index)
		} else {
			return nil, fmt.Errorf("column configuration must have either 'name' or 'value'")
		}
	}

	fieldNameStr := fmt.Sprintf("%v", fieldName)

	// 支持嵌套字段访问，如 "user.name"
	value := task.getNestedValue(jsonObj, fieldNameStr)

	return task.convertJSONValue(value, columnType, format), nil
}

func (task *JSONFileReaderTask) getNestedValue(jsonObj map[string]interface{}, fieldPath string) interface{} {
	// 支持点号分隔的嵌套字段访问
	parts := strings.Split(fieldPath, ".")
	var current interface{} = jsonObj

	for _, part := range parts {
		if currentMap, ok := current.(map[string]interface{}); ok {
			if value, exists := currentMap[part]; exists {
				current = value
			} else {
				return nil // 字段不存在
			}
		} else {
			return nil // 不是对象，无法继续访问
		}
	}

	return current
}

func (task *JSONFileReaderTask) convertJSONValue(value interface{}, columnType, format string) element.Column {
	// 检查是否为null值
	if value == nil {
		return element.NewStringColumn("")
	}

	// 如果null格式定义了，检查字符串表示
	if task.readerJob.nullFormat != "" {
		if str, ok := value.(string); ok && str == task.readerJob.nullFormat {
			return element.NewStringColumn("")
		}
	}

	switch columnType {
	case "long":
		switch v := value.(type) {
		case int64:
			return element.NewLongColumn(v)
		case int:
			return element.NewLongColumn(int64(v))
		case float64:
			return element.NewLongColumn(int64(v))
		case string:
			if intVal, err := strconv.ParseInt(v, 10, 64); err == nil {
				return element.NewLongColumn(intVal)
			}
		}
		return element.NewLongColumn(0)

	case "double":
		switch v := value.(type) {
		case float64:
			return element.NewDoubleColumn(v)
		case int64:
			return element.NewDoubleColumn(float64(v))
		case int:
			return element.NewDoubleColumn(float64(v))
		case string:
			if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
				return element.NewDoubleColumn(floatVal)
			}
		}
		return element.NewDoubleColumn(0.0)

	case "boolean":
		switch v := value.(type) {
		case bool:
			return element.NewBoolColumn(v)
		case string:
			if boolVal, err := strconv.ParseBool(v); err == nil {
				return element.NewBoolColumn(boolVal)
			}
		}
		return element.NewBoolColumn(false)

	case "date":
		valueStr := fmt.Sprintf("%v", value)
		if format != "" {
			if dateVal, err := time.Parse(format, valueStr); err == nil {
				return element.NewDateColumn(dateVal)
			}
		}
		// 尝试常见的日期格式
		dateFormats := []string{
			time.RFC3339,
			"2006-01-02T15:04:05Z",
			"2006-01-02 15:04:05",
			"2006-01-02",
			"2006/01/02 15:04:05",
			"2006/01/02",
			"01/02/2006 15:04:05",
			"01/02/2006",
		}
		for _, fmt := range dateFormats {
			if dateVal, err := time.Parse(fmt, valueStr); err == nil {
				return element.NewDateColumn(dateVal)
			}
		}
		return element.NewStringColumn(valueStr)

	default: // string
		return element.NewStringColumn(fmt.Sprintf("%v", value))
	}
}

func (task *JSONFileReaderTask) Post() error {
	return nil
}

func (task *JSONFileReaderTask) Destroy() error {
	return nil
}