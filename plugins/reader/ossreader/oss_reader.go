package ossreader

import (
	"compress/bzip2"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/factory"
	"github.com/longkeyy/go-datax/common/plugin"
)

// OssReaderJob OSS读取作业，完全复制Java版本的功能
type OssReaderJob struct {
	config           config.Configuration
	originConfig     config.Configuration

	// OSS连接参数
	endpoint    string
	accessId    string
	accessKey   string
	bucket      string
	objects     []string
	cname       bool
	objectSizePairs []ObjectSizePair

	// 文件处理参数
	columns          []map[string]interface{}
	fieldDelimiter   string
	encoding         string
	compress         string
	skipHeader       bool
	nullFormat       string
	csvReaderConfig  map[string]interface{}
	dateFormat       string
	fileFormat       string
	successOnNoObject bool
	isBinaryFile     bool

	// 代理配置
	proxyHost       string
	proxyPort       int
	proxyUsername   string
	proxyPassword   string
	proxyDomain     string
	proxyWorkstation string

	ossClient *oss.Client
	factory   *factory.DataXFactory
}

// ObjectSizePair 对象及其大小信息
type ObjectSizePair struct {
	Key  string
	Size int64
}

// SliceConfig 切片配置
type SliceConfig struct {
	FilePath string `json:"filePath"`
	Start    int64  `json:"start"`
	End      int64  `json:"end"`
}

func NewOssReaderJob() *OssReaderJob {
	return &OssReaderJob{
		factory: factory.GetGlobalFactory(),
		csvReaderConfig: map[string]interface{}{
			"safetySwitch":     false,
			"skipEmptyRecords": false,
			"useTextQualifier": false,
		},
		// 设置默认值
		fieldDelimiter:     DefaultFieldDelimiter,
		encoding:           DefaultEncoding,
		skipHeader:         DefaultSkipHeader,
		nullFormat:         DefaultNullFormat,
		fileFormat:         DefaultFileFormat,
		successOnNoObject:  DefaultSuccessOnNoObject,
	}
}

func (job *OssReaderJob) Init(config config.Configuration) error {
	job.originConfig = config
	job.config = config

	// 验证基础参数
	if err := job.basicValidateParameter(); err != nil {
		return err
	}

	// 验证文件格式和设置二进制文件标志
	job.fileFormat = job.config.GetStringWithDefault("parameter." + KeyFileFormat, DefaultFileFormat)
	job.isBinaryFile = job.isFileFormatBinary()

	// 验证其他参数
	if err := job.validate(); err != nil {
		return err
	}

	log.Printf("OssReader initialized successfully: endpoint=%s, bucket=%s", job.endpoint, job.bucket)
	return nil
}

func (job *OssReaderJob) basicValidateParameter() error {
	// 验证endpoint
	job.endpoint = job.config.GetString("parameter." + KeyEndpoint)
	if job.endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}

	// 验证accessId
	job.accessId = job.config.GetString("parameter." + KeyAccessId)
	if job.accessId == "" {
		return fmt.Errorf("accessId is required")
	}

	// 验证accessKey
	job.accessKey = job.config.GetString("parameter." + KeyAccessKey)
	if job.accessKey == "" {
		return fmt.Errorf("accessKey is required")
	}

	return nil
}

func (job *OssReaderJob) validate() error {
	// 创建OSS客户端
	if err := job.initOssClient(); err != nil {
		return err
	}

	// 验证bucket
	job.bucket = job.config.GetString("parameter." + KeyBucket)
	if job.bucket == "" {
		return fmt.Errorf("bucket is required")
	}

	// 检查bucket是否存在
	ctx := context.Background()
	exists, err := job.ossClient.IsBucketExist(ctx, job.bucket)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %v", err)
	}
	if !exists {
		return fmt.Errorf("bucket [%s] does not exist", job.bucket)
	}

	// 验证object配置
	objectsConfig := job.config.GetList("parameter." + KeyObject)
	if len(objectsConfig) == 0 {
		// 尝试获取单个object字符串
		if objectStr := job.config.GetString("parameter." + KeyObject); objectStr != "" {
			job.objects = []string{objectStr}
		} else {
			return fmt.Errorf("object is required")
		}
	} else {
		for _, obj := range objectsConfig {
			if objStr, ok := obj.(string); ok {
				job.objects = append(job.objects, objStr)
			}
		}
	}

	if len(job.objects) == 0 {
		return fmt.Errorf("no valid objects configured")
	}

	// 获取其他配置
	job.successOnNoObject = job.config.GetBoolWithDefault("parameter." + KeySuccessOnNoObject, DefaultSuccessOnNoObject)
	job.fieldDelimiter = job.config.GetStringWithDefault("parameter." + KeyFieldDelimiter, DefaultFieldDelimiter)
	job.encoding = job.config.GetStringWithDefault("parameter." + KeyEncoding, DefaultEncoding)
	job.compress = job.config.GetStringWithDefault("parameter." + KeyCompress, DefaultCompress)
	job.skipHeader = job.config.GetBoolWithDefault("parameter." + KeySkipHeader, DefaultSkipHeader)
	job.nullFormat = job.config.GetStringWithDefault("parameter." + KeyNullFormat, DefaultNullFormat)
	job.dateFormat = job.config.GetString("parameter." + KeyDateFormat)

	// 获取列配置
	columnsConfig := job.config.GetList("parameter." + KeyColumn)
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

	// 对于二进制文件，跳过文本文件验证
	if job.isBinaryFile {
		return nil
	}

	// 验证CSV相关配置
	return job.validateCsvReaderConfig()
}

func (job *OssReaderJob) initOssClient() error {
	provider := credentials.NewStaticCredentialsProvider(job.accessId, job.accessKey)

	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(provider).
		WithRegion("").  // OSS 使用endpoint，不需要region
		WithEndpoint(job.endpoint)

	if job.cname {
		cfg = cfg.WithUseCName(true)
	}

	client := oss.NewClient(cfg)
	job.ossClient = client
	return nil
}

func (job *OssReaderJob) isFileFormatBinary() bool {
	// 根据文件格式判断是否为二进制文件
	switch strings.ToLower(job.fileFormat) {
	case "parquet", "orc", "binary":
		return true
	default:
		return false
	}
}

func (job *OssReaderJob) validateCsvReaderConfig() error {
	// 这里可以添加更多的CSV配置验证逻辑
	return nil
}

func (job *OssReaderJob) Prepare() error {
	// 解析原始对象列表并获取大小信息
	objectSizePairs, err := job.parseOriginObjectSizePairs(job.objects)
	if err != nil {
		return err
	}
	job.objectSizePairs = objectSizePairs

	// 提取对象名称列表
	var objectNames []string
	for _, pair := range objectSizePairs {
		objectNames = append(objectNames, pair.Key)
	}
	job.objects = objectNames

	log.Printf("OssReader prepared with %d objects", len(job.objects))
	return nil
}

func (job *OssReaderJob) parseOriginObjectSizePairs(originObjects []string) ([]ObjectSizePair, error) {
	var parsedObjectSizePairs []ObjectSizePair
	ctx := context.Background()

	for _, object := range originObjects {
		// 检查是否包含通配符
		if strings.Contains(object, "*") || strings.Contains(object, "?") {
			// 处理通配符匹配
			firstMetaChar := strings.IndexAny(object, "*?")
			if firstMetaChar == -1 {
				firstMetaChar = len(object)
			}

			lastDirSeparator := strings.LastIndex(object[:firstMetaChar], "/")
			var parentDir string
			if lastDirSeparator >= 0 {
				parentDir = object[:lastDirSeparator+1]
			} else {
				parentDir = ""
			}

			// 获取目录下所有对象
			allRemoteObjects, err := job.getAllRemoteObjectsInDir(parentDir)
			if err != nil {
				return nil, err
			}

			// 创建正则表达式模式
			pattern := strings.ReplaceAll(object, "*", ".*")
			pattern = strings.ReplaceAll(pattern, "?", ".?")
			regex, err := regexp.Compile(pattern)
			if err != nil {
				return nil, fmt.Errorf("failed to compile regex pattern %s: %v", pattern, err)
			}

			// 匹配对象
			for _, remoteObject := range allRemoteObjects {
				if regex.MatchString(remoteObject.Key) {
					parsedObjectSizePairs = append(parsedObjectSizePairs, remoteObject)
					log.Printf("Add object [%s] as a candidate to be read", remoteObject.Key)
				}
			}
		} else {
			// 直接验证对象是否存在
			headReq := &oss.HeadObjectRequest{
				Bucket: oss.Ptr(job.bucket),
				Key:    oss.Ptr(object),
			}

			result, err := job.ossClient.HeadObject(ctx, headReq)
			if err != nil {
				if job.successOnNoObject {
					log.Printf("Warning: OSS object [%s] does not exist, skipping due to successOnNoObject=true", object)
					continue
				}
				return nil, job.trackOssDetailException(err, object)
			}

			// 如果文件大小小于等于阈值，设置为-1表示不分片
			size := result.ContentLength
			if size <= SingleFileSplitThresholdInSize {
				size = -1
			}

			parsedObjectSizePairs = append(parsedObjectSizePairs, ObjectSizePair{
				Key:  object,
				Size: size,
			})
			log.Printf("Add object [%s] as a candidate to be read", object)
		}
	}

	return parsedObjectSizePairs, nil
}

func (job *OssReaderJob) getAllRemoteObjectsInDir(parentDir string) ([]ObjectSizePair, error) {
	var objectSizePairs []ObjectSizePair
	ctx := context.Background()

	listReq := &oss.ListObjectsV2Request{
		Bucket: oss.Ptr(job.bucket),
		Prefix: oss.Ptr(parentDir),
	}

	// 使用循环方式获取所有对象
	for {
		result, err := job.ossClient.ListObjectsV2(ctx, listReq)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %v", err)
		}

		for _, object := range result.Contents {
			size := object.Size
			if size <= SingleFileSplitThresholdInSize {
				size = -1
			}
			objectSizePairs = append(objectSizePairs, ObjectSizePair{
				Key:  *object.Key,
				Size: size,
			})
		}

		if !result.IsTruncated {
			break
		}
		listReq.ContinuationToken = result.NextContinuationToken
	}

	return objectSizePairs, nil
}

func (job *OssReaderJob) trackOssDetailException(err error, object string) error {
	errMsg := err.Error()

	if strings.Contains(errMsg, "UnknownHost") {
		return fmt.Errorf("the endpoint you configured is not correct. Please check the endpoint configuration: %v", err)
	} else if strings.Contains(errMsg, "InvalidAccessKeyId") {
		return fmt.Errorf("the accessId you configured is not correct. Please check the accessId configuration: %v", err)
	} else if strings.Contains(errMsg, "SignatureDoesNotMatch") {
		return fmt.Errorf("the accessKey you configured is not correct. Please check the accessKey configuration: %v", err)
	} else if strings.Contains(errMsg, "NoSuchKey") {
		if job.successOnNoObject {
			log.Printf("Warning: OSS object [%s] does not exist, skipping due to successOnNoObject=true", object)
			return nil
		}
		return fmt.Errorf("the object [%s] you configured does not exist. Please check the object configuration", object)
	} else {
		return fmt.Errorf("please check whether the configuration of [endpoint], [accessId], [accessKey], [bucket], and [object] are correct. Error reason: %v", err)
	}
}

func (job *OssReaderJob) Split(adviceNumber int) ([]config.Configuration, error) {
	log.Printf("Starting split with advice number: %d", adviceNumber)

	var readerSplitConfigs []config.Configuration

	// 检查是否没有对象且配置了successOnNoObject
	if len(job.objects) == 0 && job.successOnNoObject {
		// 创建一个空的配置
		splitConfig := job.originConfig.Clone()
		splitConfig.Set("parameter.splitSliceConfig", nil)
		readerSplitConfigs = append(readerSplitConfigs, splitConfig)
		log.Printf("No OSS object to be read, but successOnNoObject=true")
		return readerSplitConfigs, nil
	} else if len(job.objects) == 0 {
		return nil, fmt.Errorf("unable to find the object to read. Please confirm your configured bucket: %s, object: %v", job.bucket, job.objects)
	}

	// 进行对象分片
	readerSplitConfigs = job.getSplitConfigurations(adviceNumber)

	// 报告实际分片数量
	if len(readerSplitConfigs) < adviceNumber {
		log.Printf("Note: During OSSReader data synchronization, one file can only be synchronized in one task. " +
			"You want to synchronize %d files and the number is less than the number of channels you configured: %d. " +
			"Therefore, DataX will actually have %d sub-tasks, that is, the actual concurrent channels = %d",
			len(job.objects), adviceNumber, len(readerSplitConfigs), len(readerSplitConfigs))
	}

	log.Printf("Split completed, generated %d configurations", len(readerSplitConfigs))
	return readerSplitConfigs, nil
}

func (job *OssReaderJob) getSplitConfigurations(adviceNumber int) []config.Configuration {
	var splitConfigs []config.Configuration

	// 简化版本：每个对象一个任务配置
	// 未来可以根据文件大小进行更复杂的分片
	for _, objectSizePair := range job.objectSizePairs {
		sliceConfig := SliceConfig{
			FilePath: objectSizePair.Key,
			Start:    0,
			End:      -1, // -1表示整个文件
		}

		sliceConfigs := []SliceConfig{sliceConfig}
		sliceConfigJson, _ := json.Marshal(sliceConfigs)

		splitConfig := job.originConfig.Clone()
		splitConfig.Set("parameter.splitSliceConfig", string(sliceConfigJson))
		splitConfigs = append(splitConfigs, splitConfig)
	}

	return splitConfigs
}

func (job *OssReaderJob) Post() error {
	log.Printf("OssReader post completed")
	return nil
}

func (job *OssReaderJob) Destroy() error {
	log.Printf("OssReader destroy completed")
	return nil
}

// OssReaderTask OSS读取任务
type OssReaderTask struct {
	config           config.Configuration
	sliceConfigs     []SliceConfig
	ossClient        *oss.Client
	isBinaryFile     bool
	blockSizeInByte  int
	originSkipHeader bool

	// OSS连接参数
	endpoint    string
	accessId    string
	accessKey   string
	bucket      string
	cname       bool

	// 文件处理参数
	columns          []map[string]interface{}
	fieldDelimiter   string
	encoding         string
	compress         string
	skipHeader       bool
	nullFormat       string
	dateFormat       string
	fileFormat       string
	successOnNoObject bool

	factory *factory.DataXFactory
}

func NewOssReaderTask() *OssReaderTask {
	return &OssReaderTask{
		factory: factory.GetGlobalFactory(),
		blockSizeInByte: 1024 * 1024, // 1MB
	}
}

func (task *OssReaderTask) Init(config config.Configuration) error {
	task.config = config

	// 获取分片配置
	sliceConfigStr := config.GetString("parameter.splitSliceConfig")
	if sliceConfigStr != "" {
		if err := json.Unmarshal([]byte(sliceConfigStr), &task.sliceConfigs); err != nil {
			return fmt.Errorf("failed to parse slice config: %v", err)
		}
	}

	// 获取OSS配置
	task.endpoint = config.GetString("parameter." + KeyEndpoint)
	task.accessId = config.GetString("parameter." + KeyAccessId)
	task.accessKey = config.GetString("parameter." + KeyAccessKey)
	task.bucket = config.GetString("parameter." + KeyBucket)
	task.cname = config.GetBoolWithDefault("parameter." + KeyCname, false)

	// 获取文件处理配置
	task.fileFormat = config.GetStringWithDefault("parameter." + KeyFileFormat, DefaultFileFormat)
	task.isBinaryFile = task.isFileFormatBinary()
	task.fieldDelimiter = config.GetStringWithDefault("parameter." + KeyFieldDelimiter, DefaultFieldDelimiter)
	task.encoding = config.GetStringWithDefault("parameter." + KeyEncoding, DefaultEncoding)
	task.compress = config.GetStringWithDefault("parameter." + KeyCompress, DefaultCompress)
	task.skipHeader = config.GetBoolWithDefault("parameter." + KeySkipHeader, DefaultSkipHeader)
	task.originSkipHeader = task.skipHeader
	task.nullFormat = config.GetStringWithDefault("parameter." + KeyNullFormat, DefaultNullFormat)
	task.dateFormat = config.GetString("parameter." + KeyDateFormat)
	task.successOnNoObject = config.GetBoolWithDefault("parameter." + KeySuccessOnNoObject, DefaultSuccessOnNoObject)

	// 获取列配置
	columnsConfig := config.GetList("parameter." + KeyColumn)
	if len(columnsConfig) == 1 {
		if columnStr, ok := columnsConfig[0].(string); ok && columnStr == "*" {
			task.columns = []map[string]interface{}{
				{"type": "string", "wildcard": true},
			}
		}
	}

	if len(task.columns) == 0 {
		for _, columnConfig := range columnsConfig {
			if column, ok := columnConfig.(map[string]interface{}); ok {
				if _, exists := column["type"]; !exists {
					column["type"] = "string"
				}
				task.columns = append(task.columns, column)
			}
		}
	}

	// 初始化OSS客户端
	if err := task.initOssClient(); err != nil {
		return err
	}

	log.Printf("OssReaderTask initialized")
	return nil
}

func (task *OssReaderTask) initOssClient() error {
	provider := credentials.NewStaticCredentialsProvider(task.accessId, task.accessKey)

	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(provider).
		WithRegion("").
		WithEndpoint(task.endpoint)

	if task.cname {
		cfg = cfg.WithUseCName(true)
	}

	client := oss.NewClient(cfg)
	task.ossClient = client
	return nil
}

func (task *OssReaderTask) isFileFormatBinary() bool {
	switch strings.ToLower(task.fileFormat) {
	case "parquet", "orc", "binary":
		return true
	default:
		return false
	}
}

func (task *OssReaderTask) Prepare() error {
	log.Printf("OssReaderTask prepare")
	return nil
}

func (task *OssReaderTask) StartRead(recordSender plugin.RecordSender) error {
	// 检查是否没有切片配置且配置了successOnNoObject
	if len(task.sliceConfigs) == 0 && task.successOnNoObject {
		return nil
	}

	ctx := context.Background()
	for _, sliceConfig := range task.sliceConfigs {
		objectKey := sliceConfig.FilePath
		start := sliceConfig.Start
		end := sliceConfig.End

		log.Printf("Reading bucket=[%s] object=[%s], range: [start=%d, end=%d]",
			task.bucket, objectKey, start, end)

		// 创建OSS输入流
		getReq := &oss.GetObjectRequest{
			Bucket: oss.Ptr(task.bucket),
			Key:    oss.Ptr(objectKey),
		}

		// 如果指定了范围，设置Range头
		if start > 0 || end > 0 {
			if end > 0 {
				getReq.Range = oss.Ptr(fmt.Sprintf("bytes=%d-%d", start, end))
			} else {
				getReq.Range = oss.Ptr(fmt.Sprintf("bytes=%d-", start))
			}
		}

		result, err := task.ossClient.GetObject(ctx, getReq)
		if err != nil {
			return fmt.Errorf("failed to get object %s: %v", objectKey, err)
		}
		defer result.Body.Close()

		// 检查是否要跳过表头（只在start=0时跳过）
		skipHeaderValue := task.originSkipHeader && (start == 0)

		// 根据文件类型处理
		if task.isBinaryFile {
			err = task.readBinaryFile(result.Body, objectKey, recordSender)
		} else {
			err = task.readTextFile(result.Body, objectKey, skipHeaderValue, recordSender)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (task *OssReaderTask) readBinaryFile(reader io.Reader, fileName string, recordSender plugin.RecordSender) error {
	buffer := make([]byte, task.blockSizeInByte)

	for {
		n, err := reader.Read(buffer)
		if err != nil {
			if err == io.EOF {
				if n > 0 {
					// 处理最后一块数据
					record := task.factory.GetRecordFactory().CreateRecord()
					record.AddColumn(element.NewBytesColumn(buffer[:n]))
					recordSender.SendRecord(record)
				}
				break
			}
			return fmt.Errorf("failed to read binary file %s: %v", fileName, err)
		}

		// 发送数据块
		record := task.factory.GetRecordFactory().CreateRecord()
		record.AddColumn(element.NewBytesColumn(buffer[:n]))
		recordSender.SendRecord(record)
	}

	return nil
}

func (task *OssReaderTask) readTextFile(reader io.Reader, fileName string, skipHeader bool, recordSender plugin.RecordSender) error {
	// 处理压缩格式
	var decompressedReader io.Reader = reader
	var err error

	switch strings.ToLower(task.compress) {
	case "gzip", "gz":
		decompressedReader, err = gzip.NewReader(reader)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %v", err)
		}
		defer decompressedReader.(*gzip.Reader).Close()
	case "bzip2", "bz2":
		decompressedReader = bzip2.NewReader(reader)
	}

	// 创建CSV读取器
	csvReader := csv.NewReader(decompressedReader)
	csvReader.Comma = rune(task.fieldDelimiter[0])
	csvReader.Comment = '#'
	csvReader.TrimLeadingSpace = true

	lineNumber := 0
	isWildcard := len(task.columns) == 1 && task.columns[0]["wildcard"] != nil

	for {
		fields, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read CSV from %s at line %d: %v", fileName, lineNumber+1, err)
		}

		lineNumber++

		// 跳过表头
		if skipHeader && lineNumber == 1 {
			continue
		}

		// 创建记录
		record := task.factory.GetRecordFactory().CreateRecord()

		if isWildcard {
			// 通配符模式：所有字段都作为字符串
			for _, field := range fields {
				if field == task.nullFormat {
					record.AddColumn(element.NewStringColumn(""))
				} else {
					record.AddColumn(element.NewStringColumn(field))
				}
			}
		} else {
			// 按列配置处理
			for i, column := range task.columns {
				var fieldValue string
				if i < len(fields) {
					fieldValue = fields[i]
				} else {
					fieldValue = ""
				}

				// 处理空值
				if fieldValue == task.nullFormat {
					record.AddColumn(element.NewStringColumn(""))
					continue
				}

				// 根据列类型转换
				columnType, _ := column["type"].(string)
				switch strings.ToLower(columnType) {
				case "string":
					record.AddColumn(element.NewStringColumn(fieldValue))
				case "long", "bigint", "int":
					if fieldValue == "" {
						record.AddColumn(element.NewLongColumn(0))
					} else {
						if intVal, err := strconv.ParseInt(fieldValue, 10, 64); err == nil {
							record.AddColumn(element.NewLongColumn(intVal))
						} else {
							record.AddColumn(element.NewStringColumn(fieldValue))
						}
					}
				case "double", "float":
					if fieldValue == "" {
						record.AddColumn(element.NewDoubleColumn(0.0))
					} else {
						if floatVal, err := strconv.ParseFloat(fieldValue, 64); err == nil {
							record.AddColumn(element.NewDoubleColumn(floatVal))
						} else {
							record.AddColumn(element.NewStringColumn(fieldValue))
						}
					}
				case "bool", "boolean":
					if fieldValue == "" {
						record.AddColumn(element.NewBoolColumn(false))
					} else {
						if boolVal, err := strconv.ParseBool(fieldValue); err == nil {
							record.AddColumn(element.NewBoolColumn(boolVal))
						} else {
							record.AddColumn(element.NewStringColumn(fieldValue))
						}
					}
				case "date", "datetime", "timestamp":
					if fieldValue == "" {
						record.AddColumn(element.NewDateColumn(time.Time{}))
					} else {
						if task.dateFormat != "" {
							if dateVal, err := time.Parse(task.dateFormat, fieldValue); err == nil {
								record.AddColumn(element.NewDateColumn(dateVal))
							} else {
								record.AddColumn(element.NewStringColumn(fieldValue))
							}
						} else {
							record.AddColumn(element.NewStringColumn(fieldValue))
						}
					}
				default:
					record.AddColumn(element.NewStringColumn(fieldValue))
				}
			}
		}

		// 发送记录
		recordSender.SendRecord(record)
	}

	log.Printf("Read %d lines from object %s", lineNumber, fileName)
	return nil
}

func (task *OssReaderTask) Post() error {
	log.Printf("OssReaderTask post")
	return nil
}

func (task *OssReaderTask) Destroy() error {
	log.Printf("OssReaderTask destroy")
	return nil
}