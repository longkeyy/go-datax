package ftpreader

import (
	"compress/bzip2"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/factory"
	"github.com/longkeyy/go-datax/common/plugin"
)

// FtpReaderJob FTP读取作业，复制Java版本的功能
type FtpReaderJob struct {
	config           config.Configuration
	originConfig     config.Configuration
	paths            []string
	columns          []map[string]interface{}
	sourceFiles      []string

	// FTP连接参数
	protocol          string
	host             string
	port             int
	username         string
	password         string
	timeout          int
	connectPattern   string
	maxTraversalLevel int

	// 文件处理参数 - 复用txtfilereader的逻辑
	fieldDelimiter   string
	encoding         string
	compress         string
	skipHeader       bool
	nullFormat       string
	csvReaderConfig  map[string]interface{}

	ftpHelper FtpHelper
	factory   *factory.DataXFactory
}

func NewFtpReaderJob() *FtpReaderJob {
	return &FtpReaderJob{
		factory: factory.GetGlobalFactory(),
		csvReaderConfig: map[string]interface{}{
			"safetySwitch":     false,
			"skipEmptyRecords": false,
			"useTextQualifier": false,
		},
	}
}

func (job *FtpReaderJob) Init(config config.Configuration) error {
	job.originConfig = config
	job.config = config

	// 验证参数
	if err := job.validateParameter(); err != nil {
		return err
	}

	// 创建FTP Helper
	if err := job.createFtpHelper(); err != nil {
		return err
	}

	// 登录FTP服务器
	if err := job.ftpHelper.LoginFtpServer(job.host, job.username, job.password, job.port, job.timeout, job.connectPattern); err != nil {
		return fmt.Errorf("failed to login to FTP server: %v", err)
	}

	log.Printf("FtpReader initialized successfully: protocol=%s, host=%s, port=%d",
		job.protocol, job.host, job.port)
	return nil
}

func (job *FtpReaderJob) validateParameter() error {
	// 验证协议
	job.protocol = job.config.GetString("parameter." + KeyProtocol)
	if job.protocol == "" {
		return fmt.Errorf("protocol is required")
	}
	if job.protocol != "ftp" && job.protocol != "sftp" {
		return fmt.Errorf("仅支持 ftp和sftp 传输协议, 不支持您配置的传输协议: [%s]", job.protocol)
	}

	// 验证主机信息
	job.host = job.config.GetString("parameter." + KeyHost)
	if job.host == "" {
		return fmt.Errorf("host is required")
	}

	job.username = job.config.GetString("parameter." + KeyUsername)
	if job.username == "" {
		return fmt.Errorf("username is required")
	}

	job.password = job.config.GetString("parameter." + KeyPassword)
	if job.password == "" {
		return fmt.Errorf("password is required")
	}

	// 设置端口
	if job.protocol == "sftp" {
		job.port = job.config.GetIntWithDefault("parameter."+KeyPort, DefaultSftpPort)
	} else {
		job.port = job.config.GetIntWithDefault("parameter."+KeyPort, DefaultFtpPort)
	}

	// 其他参数
	job.timeout = job.config.GetIntWithDefault("parameter."+KeyTimeout, DefaultTimeout)
	job.maxTraversalLevel = job.config.GetIntWithDefault("parameter."+KeyMaxTraversalLevel, DefaultMaxTraversalLevel)
	job.connectPattern = job.config.GetStringWithDefault("parameter."+KeyConnectPattern, DefaultFtpConnectPattern)

	// 验证连接模式
	if job.connectPattern != "PORT" && job.connectPattern != "PASV" {
		return fmt.Errorf("不支持您配置的ftp传输模式: [%s]", job.connectPattern)
	}

	// 验证路径配置
	if err := job.validatePath(); err != nil {
		return err
	}

	// 验证列配置
	if err := job.validateColumn(); err != nil {
		return err
	}

	// 获取文件处理参数（继承txtfilereader逻辑）
	job.fieldDelimiter = job.config.GetStringWithDefault("parameter."+KeyFieldDelimiter, DefaultFieldDelimiter)
	job.encoding = job.config.GetStringWithDefault("parameter."+KeyEncoding, DefaultEncoding)
	job.compress = job.config.GetString("parameter." + KeyCompress)
	job.skipHeader = job.config.GetBoolWithDefault("parameter."+KeySkipHeader, false)
	job.nullFormat = job.config.GetStringWithDefault("parameter."+KeyNullFormat, DefaultNullFormat)

	// 获取csvReaderConfig
	if csvConfig := job.config.GetMap("parameter." + KeyCsvReaderConfig); csvConfig != nil {
		job.csvReaderConfig = csvConfig
	}

	return nil
}

func (job *FtpReaderJob) validatePath() error {
	// 获取路径配置 - 与Java版本一致
	pathConfig := job.config.GetString("parameter." + KeyPath)
	if pathConfig == "" {
		return fmt.Errorf("您需要指定待读取的源目录或文件")
	}

	// 检查是否为数组格式
	if strings.HasPrefix(pathConfig, "[") && strings.HasSuffix(pathConfig, "]") {
		// 处理JSON数组格式
		pathList := job.config.GetList("parameter." + KeyPath)
		if len(pathList) == 0 {
			return fmt.Errorf("您需要指定待读取的源目录或文件")
		}
		for _, pathItem := range pathList {
			if pathStr, ok := pathItem.(string); ok {
				if !strings.HasPrefix(pathStr, "/") {
					return fmt.Errorf("请检查参数path:[%s],需要配置为绝对路径", pathStr)
				}
				job.paths = append(job.paths, pathStr)
			}
		}
	} else {
		// 单个路径
		if !strings.HasPrefix(pathConfig, "/") {
			return fmt.Errorf("请检查参数path:[%s],需要配置为绝对路径", pathConfig)
		}
		job.paths = []string{pathConfig}
	}

	return nil
}

func (job *FtpReaderJob) validateColumn() error {
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
			return nil
		}
	}

	// 处理具体列配置
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

	return nil
}

func (job *FtpReaderJob) createFtpHelper() error {
	switch job.protocol {
	case "sftp":
		job.ftpHelper = NewSftpHelper()
	case "ftp":
		job.ftpHelper = NewStandardFtpHelper()
	default:
		return fmt.Errorf("unsupported protocol: %s", job.protocol)
	}
	return nil
}

func (job *FtpReaderJob) Prepare() error {
	log.Printf("prepare() begin...")

	// 获取所有文件列表
	sourceFiles, err := job.ftpHelper.GetAllFiles(job.paths, 0, job.maxTraversalLevel)
	if err != nil {
		return fmt.Errorf("failed to get file list: %v", err)
	}

	job.sourceFiles = sourceFiles
	log.Printf("您即将读取的文件数为: [%d]", len(job.sourceFiles))
	return nil
}

func (job *FtpReaderJob) Split(adviceNumber int) ([]config.Configuration, error) {
	log.Printf("split() begin...")

	var readerSplitConfigs []config.Configuration

	// 每个slice处理一个文件 - 与Java版本一致
	splitNumber := len(job.sourceFiles)
	if splitNumber == 0 {
		return nil, fmt.Errorf("未能找到待读取的文件,请确认您的配置项path: %v", job.paths)
	}

	// 分割源文件
	splitedSourceFiles := job.splitSourceFiles(job.sourceFiles, adviceNumber)
	for i, files := range splitedSourceFiles {
		splitedConfig := job.config.Clone()
		splitedConfig.Set(SourceFiles, files)
		splitedConfig.Set("taskId", i)
		readerSplitConfigs = append(readerSplitConfigs, splitedConfig)
	}

	log.Printf("split() ok and end... tasks=%d", len(readerSplitConfigs))
	return readerSplitConfigs, nil
}

func (job *FtpReaderJob) splitSourceFiles(sourceList []string, adviceNumber int) [][]string {
	var splitedList [][]string
	listSize := len(sourceList)

	if adviceNumber > listSize {
		adviceNumber = listSize
	}

	averageLength := listSize / adviceNumber
	if averageLength == 0 {
		averageLength = 1
	}

	for begin := 0; begin < listSize; {
		end := begin + averageLength
		if end > listSize {
			end = listSize
		}
		splitedList = append(splitedList, sourceList[begin:end])
		begin = end
	}

	return splitedList
}

func (job *FtpReaderJob) Post() error {
	return nil
}

func (job *FtpReaderJob) Destroy() error {
	if job.ftpHelper != nil {
		if err := job.ftpHelper.LogoutFtpServer(); err != nil {
			log.Printf("关闭与ftp服务器连接失败: error=%v, host=%s, username=%s, port=%d",
				err, job.host, job.username, job.port)
		}
	}
	return nil
}

// FtpReaderTask FTP读取任务
type FtpReaderTask struct {
	config              config.Configuration
	readerSliceConfig   config.Configuration
	sourceFiles         []string

	// FTP连接参数
	host             string
	port             int
	username         string
	password         string
	protocol         string
	timeout          int
	connectPattern   string

	// 文件处理参数
	fieldDelimiter   string
	encoding         string
	compress         string
	skipHeader       bool
	nullFormat       string
	csvReaderConfig  map[string]interface{}
	columns          []map[string]interface{}

	ftpHelper FtpHelper
	factory   *factory.DataXFactory
}

func NewFtpReaderTask() *FtpReaderTask {
	return &FtpReaderTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *FtpReaderTask) Init(config config.Configuration) error {
	task.readerSliceConfig = config
	task.config = config

	// 获取FTP连接参数
	task.host = config.GetString("parameter." + KeyHost)
	task.protocol = config.GetString("parameter." + KeyProtocol)
	task.username = config.GetString("parameter." + KeyUsername)
	task.password = config.GetString("parameter." + KeyPassword)
	task.timeout = config.GetIntWithDefault("parameter."+KeyTimeout, DefaultTimeout)

	// 设置端口
	if task.protocol == "sftp" {
		task.port = config.GetIntWithDefault("parameter."+KeyPort, DefaultSftpPort)
	} else {
		task.port = config.GetIntWithDefault("parameter."+KeyPort, DefaultFtpPort)
		task.connectPattern = config.GetStringWithDefault("parameter."+KeyConnectPattern, DefaultFtpConnectPattern)
	}

	// 获取文件列表
	if sourceFiles := config.GetList(SourceFiles); sourceFiles != nil {
		for _, file := range sourceFiles {
			if fileStr, ok := file.(string); ok {
				task.sourceFiles = append(task.sourceFiles, fileStr)
			}
		}
	}

	// 获取文件处理参数
	task.fieldDelimiter = config.GetStringWithDefault("parameter."+KeyFieldDelimiter, DefaultFieldDelimiter)
	task.encoding = config.GetStringWithDefault("parameter."+KeyEncoding, DefaultEncoding)
	task.compress = config.GetString("parameter." + KeyCompress)
	task.skipHeader = config.GetBoolWithDefault("parameter."+KeySkipHeader, false)
	task.nullFormat = config.GetStringWithDefault("parameter."+KeyNullFormat, DefaultNullFormat)

	// 获取列配置
	if err := task.parseColumns(); err != nil {
		return err
	}

	// 获取csvReaderConfig
	task.csvReaderConfig = make(map[string]interface{})
	if csvConfig := config.GetMap("parameter." + KeyCsvReaderConfig); csvConfig != nil {
		task.csvReaderConfig = csvConfig
	}

	// 创建FTP Helper并登录
	if err := task.createAndConnectFtpHelper(); err != nil {
		return err
	}

	log.Printf("FtpReader task initialized with %d files", len(task.sourceFiles))
	return nil
}

func (task *FtpReaderTask) parseColumns() error {
	columnsConfig := task.config.GetList("parameter." + KeyColumn)
	if len(columnsConfig) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	// 检查通配符
	if len(columnsConfig) == 1 {
		if columnStr, ok := columnsConfig[0].(string); ok && columnStr == "*" {
			task.columns = []map[string]interface{}{
				{"type": "string", "wildcard": true},
			}
			return nil
		}
	}

	// 处理具体列配置
	for _, columnConfig := range columnsConfig {
		if column, ok := columnConfig.(map[string]interface{}); ok {
			if _, exists := column["type"]; !exists {
				column["type"] = "string"
			}
			task.columns = append(task.columns, column)
		}
	}

	return nil
}

func (task *FtpReaderTask) createAndConnectFtpHelper() error {
	switch task.protocol {
	case "sftp":
		task.ftpHelper = NewSftpHelper()
	case "ftp":
		task.ftpHelper = NewStandardFtpHelper()
	default:
		return fmt.Errorf("unsupported protocol: %s", task.protocol)
	}

	return task.ftpHelper.LoginFtpServer(task.host, task.username, task.password, task.port, task.timeout, task.connectPattern)
}

func (task *FtpReaderTask) Prepare() error {
	return nil
}

func (task *FtpReaderTask) StartRead(recordSender plugin.RecordSender) error {
	log.Printf("start read source files...")

	for _, fileName := range task.sourceFiles {
		log.Printf("reading file : [%s]", fileName)

		inputStream, err := task.ftpHelper.GetInputStream(fileName)
		if err != nil {
			return fmt.Errorf("failed to get input stream for file %s: %v", fileName, err)
		}

		err = task.readFromStream(inputStream, fileName, recordSender)
		inputStream.Close()

		if err != nil {
			return fmt.Errorf("failed to read from file %s: %v", fileName, err)
		}

		// 刷新记录
		if err := recordSender.Flush(); err != nil {
			return fmt.Errorf("failed to flush records: %v", err)
		}
	}

	log.Printf("end read source files...")
	return nil
}

func (task *FtpReaderTask) readFromStream(inputStream io.Reader, fileName string, recordSender plugin.RecordSender) error {
	// 处理压缩
	var reader io.Reader = inputStream
	switch task.compress {
	case "gzip":
		gzipReader, err := gzip.NewReader(inputStream)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %v", err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	case "bzip2":
		reader = bzip2.NewReader(inputStream)
	case "zip":
		return fmt.Errorf("zip compression not supported for single file reading")
	}

	// 创建CSV读取器
	csvReader := csv.NewReader(reader)

	// 设置分隔符
	if task.fieldDelimiter == "\\t" {
		csvReader.Comma = '\t'
	} else if len(task.fieldDelimiter) > 0 {
		csvReader.Comma = rune(task.fieldDelimiter[0])
	}

	// 配置CSV读取器
	if useTextQualifier, ok := task.csvReaderConfig["useTextQualifier"].(bool); ok && !useTextQualifier {
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
			return fmt.Errorf("failed to read CSV record at line %d: %v", lineNumber+1, err)
		}

		lineNumber++

		// 跳过表头
		if task.skipHeader && lineNumber == 1 {
			continue
		}

		// 跳过空行
		if skipEmpty, ok := task.csvReaderConfig["skipEmptyRecords"].(bool); ok && skipEmpty {
			if len(record) == 0 || (len(record) == 1 && strings.TrimSpace(record[0]) == "") {
				continue
			}
		}

		// 转换为DataX记录
		dataxRecord, err := task.convertRecord(record)
		if err != nil {
			return fmt.Errorf("failed to convert record at line %d: %v", lineNumber, err)
		}

		// 发送记录
		if err := recordSender.SendRecord(dataxRecord); err != nil {
			return fmt.Errorf("failed to send record: %v", err)
		}

		recordCount++

		// 每1000条记录输出一次进度
		if recordCount%1000 == 0 {
			log.Printf("Read %d records from %s", recordCount, fileName)
		}
	}

	return nil
}

// convertRecord 转换CSV记录为DataX记录，复用txtfilereader逻辑
func (task *FtpReaderTask) convertRecord(csvRecord []string) (element.Record, error) {
	record := task.factory.GetRecordFactory().CreateRecord()

	// 处理通配符模式
	if len(task.columns) == 1 {
		if _, isWildcard := task.columns[0]["wildcard"]; isWildcard {
			// 通配符模式：所有字段都作为string
			for _, value := range csvRecord {
				column := task.convertValue(value, "string", "")
				record.AddColumn(column)
			}
			return record, nil
		}
	}

	// 常规模式：按配置的列信息处理
	for _, columnConfig := range task.columns {
		column, err := task.convertColumnValue(csvRecord, columnConfig)
		if err != nil {
			return nil, err
		}
		record.AddColumn(column)
	}

	return record, nil
}

func (task *FtpReaderTask) convertColumnValue(csvRecord []string, columnConfig map[string]interface{}) (element.Column, error) {
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
		return task.factory.GetColumnFactory().CreateStringColumn(""), nil
	}

	value := csvRecord[indexInt]
	return task.convertValue(value, columnType, format), nil
}

func (task *FtpReaderTask) convertValue(value, columnType, format string) element.Column {
	// 检查是否为null值
	if value == task.nullFormat || value == "" {
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

func (task *FtpReaderTask) Post() error {
	return nil
}

func (task *FtpReaderTask) Destroy() error {
	if task.ftpHelper != nil {
		if err := task.ftpHelper.LogoutFtpServer(); err != nil {
			log.Printf("关闭与ftp服务器连接失败: error=%v, host=%s, username=%s, port=%d",
				err, task.host, task.username, task.port)
		}
	}
	return nil
}