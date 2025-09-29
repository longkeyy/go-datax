package ftpwriter

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"log"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/factory"
	"github.com/longkeyy/go-datax/common/plugin"
)

// FtpWriterJob FTP写入作业，复制Java版本的功能
type FtpWriterJob struct {
	config           config.Configuration
	writerSliceConfig config.Configuration
	allFileExists    map[string]bool

	// FTP连接参数
	protocol string
	host     string
	port     int
	username string
	password string
	timeout  int

	// 文件写入参数
	path           string
	fileName       string
	writeMode      string
	fieldDelimiter string
	encoding       string
	compress       string
	nullFormat     string
	dateFormat     string
	fileFormat     string
	header         []string
	suffix         string

	ftpHelper IFtpHelper
	factory   *factory.DataXFactory
}

func NewFtpWriterJob() *FtpWriterJob {
	return &FtpWriterJob{
		factory:        factory.GetGlobalFactory(),
		fieldDelimiter: DefaultFieldDelimiter,
		encoding:       DefaultEncoding,
		nullFormat:     DefaultNullFormat,
		fileFormat:     DefaultFileFormat,
		allFileExists:  make(map[string]bool),
	}
}

func (job *FtpWriterJob) Init(config config.Configuration) error {
	job.writerSliceConfig = config
	job.config = config

	// 验证参数
	if err := job.validateParameter(); err != nil {
		return err
	}

	// 创建FTP Helper并登录（带重试机制）
	if err := job.createFtpHelperAndLogin(); err != nil {
		return err
	}

	log.Printf("FtpWriter initialized successfully: protocol=%s, host=%s, port=%d",
		job.protocol, job.host, job.port)
	return nil
}

func (job *FtpWriterJob) validateParameter() error {
	// 验证文件名
	job.fileName = job.config.GetString("parameter." + KeyFileName)
	if job.fileName == "" {
		return fmt.Errorf("fileName is required")
	}

	// 验证路径
	job.path = job.config.GetString("parameter." + KeyPath)
	if job.path == "" {
		return fmt.Errorf("path is required")
	}
	if !strings.HasPrefix(job.path, "/") {
		return fmt.Errorf("请检查参数path:%s,需要配置为绝对路径", job.path)
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

	job.timeout = job.config.GetIntWithDefault("parameter."+KeyTimeout, DefaultTimeout)

	// 验证协议并设置端口
	job.protocol = job.config.GetString("parameter." + KeyProtocol)
	if job.protocol == "" {
		return fmt.Errorf("protocol is required")
	}

	switch strings.ToLower(job.protocol) {
	case "sftp":
		job.port = job.config.GetIntWithDefault("parameter."+KeyPort, DefaultSftpPort)
		job.ftpHelper = NewSftpHelperImpl()
	case "ftp":
		job.port = job.config.GetIntWithDefault("parameter."+KeyPort, DefaultFtpPort)
		job.ftpHelper = NewStandardFtpHelperImpl()
	default:
		return fmt.Errorf("仅支持 ftp和sftp 传输协议, 不支持您配置的传输协议: [%s]", job.protocol)
	}

	// 获取可选参数
	job.writeMode = job.config.GetString("parameter." + KeyWriteMode)
	job.fieldDelimiter = job.config.GetStringWithDefault("parameter."+KeyFieldDelimiter, DefaultFieldDelimiter)
	job.encoding = job.config.GetStringWithDefault("parameter."+KeyEncoding, DefaultEncoding)
	job.compress = job.config.GetString("parameter." + KeyCompress)
	job.nullFormat = job.config.GetStringWithDefault("parameter."+KeyNullFormat, DefaultNullFormat)
	job.dateFormat = job.config.GetString("parameter." + KeyDateFormat)
	job.fileFormat = job.config.GetStringWithDefault("parameter."+KeyFileFormat, DefaultFileFormat)
	job.suffix = job.config.GetString("parameter." + KeySuffix)

	// 获取header配置
	headerList := job.config.GetList("parameter." + KeyHeader)
	for _, h := range headerList {
		if headerStr, ok := h.(string); ok {
			job.header = append(job.header, headerStr)
		}
	}

	// 设置端口到配置中
	job.config.Set("parameter."+KeyPort, job.port)

	return nil
}

func (job *FtpWriterJob) createFtpHelperAndLogin() error {
	// 实现重试机制，对应Java版本的RetryUtil.executeWithRetry
	maxRetries := 3
	retryDelay := 4 * time.Second

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		err := job.ftpHelper.LoginFtpServer(job.host, job.username, job.password, job.port, job.timeout)
		if err == nil {
			return nil
		}

		lastErr = err
		if i < maxRetries-1 {
			log.Printf("FTP login failed, retrying... attempt=%d, error=%v", i+1, err)
			time.Sleep(retryDelay)
		}
	}

	return fmt.Errorf("与ftp服务器建立连接失败, host:%s, username:%s, port:%d, errorMessage:%v",
		job.host, job.username, job.port, lastErr)
}

func (job *FtpWriterJob) Prepare() error {
	// 警告：这里用户需要配一个目录
	if err := job.ftpHelper.MkDirRecursive(job.path); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", job.path, err)
	}

	// 获取已存在的文件列表
	allFileExists, err := job.ftpHelper.GetAllFilesInDir(job.path, job.fileName)
	if err != nil {
		return fmt.Errorf("failed to list files in directory %s: %v", job.path, err)
	}
	job.allFileExists = allFileExists

	// 根据writeMode处理已存在的文件
	switch job.writeMode {
	case "truncate":
		log.Printf("由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的内容", job.path, job.fileName)

		fullFileNameToDelete := make(map[string]bool)
		for fileName := range allFileExists {
			fullFilePath := job.buildFilePath(job.path, fileName, "")
			fullFileNameToDelete[fullFilePath] = true
		}

		if len(fullFileNameToDelete) > 0 {
			log.Printf("删除目录path:[%s] 下指定前缀fileName:[%s] 文件列表如下: [%v]", job.path, job.fileName, getMapKeys(fullFileNameToDelete))

			if err := job.ftpHelper.DeleteFiles(fullFileNameToDelete); err != nil {
				return fmt.Errorf("failed to delete existing files: %v", err)
			}
		}

	case "append":
		log.Printf("由于您配置了writeMode append, 写入前不做清理工作, [%s] 目录下写入相应文件名前缀 [%s] 的文件", job.path, job.fileName)

	case "nonConflict":
		log.Printf("由于您配置了writeMode nonConflict, 开始检查 [%s] 下面的内容", job.path)

		if len(allFileExists) > 0 {
			log.Printf("目录path:[%s] 下指定前缀fileName:[%s] 冲突文件列表如下: [%v]", job.path, job.fileName, getMapKeys(allFileExists))

			return fmt.Errorf("您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹", job.path)
		}

	default:
		return fmt.Errorf("仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式: [%s]", job.writeMode)
	}

	return nil
}

func (job *FtpWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	// 使用类似UnstructuredStorageWriterUtil.split的逻辑
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

func (job *FtpWriterJob) Post() error {
	return nil
}

func (job *FtpWriterJob) Destroy() error {
	if job.ftpHelper != nil {
		if err := job.ftpHelper.LogoutFtpServer(); err != nil {
			log.Printf("关闭与ftp服务器连接失败: error=%v, host=%s, username=%s, port=%d",
				err, job.host, job.username, job.port)
		}
	}
	return nil
}

// 辅助函数
func (job *FtpWriterJob) buildFilePath(path, fileName, suffix string) string {
	if suffix == "" {
		return path + "/" + fileName
	}
	return path + "/" + fileName + "." + suffix
}

func getMapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// FtpWriterTask FTP写入任务
type FtpWriterTask struct {
	config              config.Configuration
	writerSliceConfig   config.Configuration

	// 文件写入参数
	path           string
	fileName       string
	suffix         string
	fieldDelimiter string
	encoding       string
	compress       string
	nullFormat     string
	dateFormat     string
	fileFormat     string
	header         []string

	// FTP连接参数
	protocol string
	host     string
	port     int
	username string
	password string
	timeout  int

	ftpHelper IFtpHelper
	factory   *factory.DataXFactory
}

func NewFtpWriterTask() *FtpWriterTask {
	return &FtpWriterTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *FtpWriterTask) Init(config config.Configuration) error {
	task.writerSliceConfig = config
	task.config = config

	// 获取文件参数
	task.path = config.GetString("parameter." + KeyPath)
	task.fileName = config.GetString("parameter." + KeyFileName)
	task.suffix = config.GetString("parameter." + KeySuffix)
	task.fieldDelimiter = config.GetStringWithDefault("parameter."+KeyFieldDelimiter, DefaultFieldDelimiter)
	task.encoding = config.GetStringWithDefault("parameter."+KeyEncoding, DefaultEncoding)
	task.compress = config.GetString("parameter." + KeyCompress)
	task.nullFormat = config.GetStringWithDefault("parameter."+KeyNullFormat, DefaultNullFormat)
	task.dateFormat = config.GetString("parameter." + KeyDateFormat)
	task.fileFormat = config.GetStringWithDefault("parameter."+KeyFileFormat, DefaultFileFormat)

	// 获取header配置
	headerList := config.GetList("parameter." + KeyHeader)
	for _, h := range headerList {
		if headerStr, ok := h.(string); ok {
			task.header = append(task.header, headerStr)
		}
	}

	// 获取FTP连接参数
	task.host = config.GetString("parameter." + KeyHost)
	task.port = config.GetInt("parameter." + KeyPort)
	task.username = config.GetString("parameter." + KeyUsername)
	task.password = config.GetString("parameter." + KeyPassword)
	task.timeout = config.GetIntWithDefault("parameter."+KeyTimeout, DefaultTimeout)
	task.protocol = config.GetString("parameter." + KeyProtocol)

	// 创建FTP Helper并登录（带重试机制）
	if err := task.createFtpHelperAndLogin(); err != nil {
		return err
	}

	log.Printf("FtpWriter task initialized successfully")
	return nil
}

func (task *FtpWriterTask) createFtpHelperAndLogin() error {
	// 创建FTP Helper
	switch strings.ToLower(task.protocol) {
	case "sftp":
		task.ftpHelper = NewSftpHelperImpl()
	case "ftp":
		task.ftpHelper = NewStandardFtpHelperImpl()
	default:
		return fmt.Errorf("unsupported protocol: %s", task.protocol)
	}

	// 实现重试机制
	maxRetries := 3
	retryDelay := 4 * time.Second

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		err := task.ftpHelper.LoginFtpServer(task.host, task.username, task.password, task.port, task.timeout)
		if err == nil {
			return nil
		}

		lastErr = err
		if i < maxRetries-1 {
			log.Printf("FTP login failed, retrying... attempt=%d, error=%v", i+1, err)
			time.Sleep(retryDelay)
		}
	}

	return fmt.Errorf("与ftp服务器建立连接失败, host:%s, username:%s, port:%d, errorMessage:%v",
		task.host, task.username, task.port, lastErr)
}

func (task *FtpWriterTask) Prepare() error {
	return nil
}

func (task *FtpWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	log.Printf("begin do write...")

	// 构建文件完整路径
	fileFullPath := task.buildFilePath(task.path, task.fileName, task.suffix)
	log.Printf("write to file : [%s]", fileFullPath)

	// 获取输出流
	outputStream, err := task.ftpHelper.GetOutputStream(fileFullPath)
	if err != nil {
		return fmt.Errorf("无法创建待写文件: [%s], error: %v", task.fileName, err)
	}
	defer outputStream.Close()

	// 写入数据
	if err := task.writeToStream(recordReceiver, outputStream); err != nil {
		return fmt.Errorf("failed to write data: %v", err)
	}

	log.Printf("end do write")
	return nil
}

func (task *FtpWriterTask) writeToStream(recordReceiver plugin.RecordReceiver, outputStream io.Writer) error {
	// 处理压缩
	var writer io.Writer = outputStream
	if task.compress == "gzip" {
		gzipWriter := gzip.NewWriter(outputStream)
		defer gzipWriter.Close()
		writer = gzipWriter
	}

	// 创建CSV写入器
	csvWriter := csv.NewWriter(writer)
	defer csvWriter.Flush()

	// 设置分隔符
	if task.fieldDelimiter == "\\t" {
		csvWriter.Comma = '\t'
	} else if len(task.fieldDelimiter) > 0 {
		csvWriter.Comma = rune(task.fieldDelimiter[0])
	}

	// 写入表头
	if len(task.header) > 0 {
		if err := csvWriter.Write(task.header); err != nil {
			return fmt.Errorf("failed to write header: %v", err)
		}
	}

	// 写入数据记录
	recordCount := 0
	for {
		record, err := recordReceiver.GetFromReader()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive record: %v", err)
		}

		// 转换记录为字符串数组
		csvRecord, err := task.convertRecord(record)
		if err != nil {
			return fmt.Errorf("failed to convert record: %v", err)
		}

		// 写入记录
		if err := csvWriter.Write(csvRecord); err != nil {
			return fmt.Errorf("failed to write record: %v", err)
		}

		recordCount++
		if recordCount%1000 == 0 {
			log.Printf("Write progress: %d records", recordCount)
		}
	}

	log.Printf("Total records written: %d", recordCount)
	return nil
}

func (task *FtpWriterTask) convertRecord(record element.Record) ([]string, error) {
	var csvRecord []string

	columnCount := record.GetColumnNumber()
	for i := 0; i < columnCount; i++ {
		column := record.GetColumn(i)
		value, err := task.convertColumn(column)
		if err != nil {
			return nil, fmt.Errorf("failed to convert column %d: %v", i, err)
		}
		csvRecord = append(csvRecord, value)
	}

	return csvRecord, nil
}

func (task *FtpWriterTask) convertColumn(column element.Column) (string, error) {
	if column == nil || column.GetRawData() == nil {
		return task.nullFormat, nil
	}

	switch column.GetType() {
	case element.TypeString:
		return column.GetAsString(), nil
	case element.TypeLong:
		if longVal, err := column.GetAsLong(); err == nil {
			return strconv.FormatInt(longVal, 10), nil
		}
		return task.nullFormat, nil
	case element.TypeDouble:
		if doubleVal, err := column.GetAsDouble(); err == nil {
			return strconv.FormatFloat(doubleVal, 'f', -1, 64), nil
		}
		return task.nullFormat, nil
	case element.TypeBool:
		if boolVal, err := column.GetAsBool(); err == nil {
			return strconv.FormatBool(boolVal), nil
		}
		return task.nullFormat, nil
	case element.TypeDate:
		if dateVal, err := column.GetAsDate(); err == nil {
			if task.dateFormat != "" {
				return dateVal.Format(task.dateFormat), nil
			}
			return dateVal.Format("2006-01-02 15:04:05"), nil
		}
		return task.nullFormat, nil
	default:
		return fmt.Sprintf("%v", column.GetRawData()), nil
	}
}

func (task *FtpWriterTask) buildFilePath(path, fileName, suffix string) string {
	// 添加任务ID到文件名，避免并发写入冲突
	taskId := task.config.GetInt("taskId")

	var fullFileName string
	if taskId >= 0 {
		if suffix == "" {
			fullFileName = fmt.Sprintf("%s_%d", fileName, taskId)
		} else {
			fullFileName = fmt.Sprintf("%s_%d.%s", fileName, taskId, suffix)
		}
	} else {
		if suffix == "" {
			fullFileName = fileName
		} else {
			fullFileName = fmt.Sprintf("%s.%s", fileName, suffix)
		}
	}

	return path + "/" + fullFileName
}

func (task *FtpWriterTask) Post() error {
	return nil
}

func (task *FtpWriterTask) Destroy() error {
	if task.ftpHelper != nil {
		if err := task.ftpHelper.LogoutFtpServer(); err != nil {
			log.Printf("关闭与ftp服务器连接失败: error=%v, host=%s, username=%s, port=%d",
				err, task.host, task.username, task.port)
		}
	}
	return nil
}