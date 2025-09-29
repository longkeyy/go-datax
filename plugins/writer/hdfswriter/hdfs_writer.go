package hdfswriter

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"log"
	"path/filepath"
	"strings"

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
	FileTypeORC     = "ORC"
	FileTypeParquet = "PARQUET"

	// 配置键
	KeyDefaultFS              = "defaultFS"
	KeyPath                   = "path"
	KeyFileType              = "fileType"
	KeyFileName              = "fileName"
	KeyColumn                = "column"
	KeyWriteMode             = "writeMode"
	KeyFieldDelimiter        = "fieldDelimiter"
	KeyEncoding              = "encoding"
	KeyCompress              = "compress"
	KeyNullFormat            = "nullFormat"
	KeyHaveKerberos          = "haveKerberos"
	KeyKerberosKeytabFilePath = "kerberosKeytabFilePath"
	KeyKerberosPrincipal     = "kerberosPrincipal"
	KeyKerberosConfFilePath  = "kerberosConfFilePath"
	KeyHadoopConfig          = "hadoopConfig"
	KeyHdfsUsername          = "hdfsUsername"

	// 写模式
	WriteModeAppend     = "append"
	WriteModeNonConflict = "nonconflict"
	WriteModeTruncate   = "truncate"

	// 默认值
	DefaultEncoding = "UTF-8"
	DefaultUsername = "admin"
)

// HdfsWriterJob HDFS文件写入作业
type HdfsWriterJob struct {
	config              config.Configuration
	defaultFS          string
	path               string
	fileType           string
	fileName           string
	columns            []map[string]interface{}
	writeMode          string
	fieldDelimiter     string
	encoding           string
	compress           string
	nullFormat         string
	haveKerberos       bool
	kerberosKeytabFile string
	kerberosPrincipal  string
	kerberosConfFile   string
	hadoopConfig       map[string]interface{}
	hdfsUsername       string
	tmpFiles           []string // 临时文件路径
	endFiles           []string // 最终文件路径
	factory            *factory.DataXFactory
}

func NewHdfsWriterJob() *HdfsWriterJob {
	return &HdfsWriterJob{
		fileType:       FileTypeText,
		writeMode:      WriteModeAppend,
		fieldDelimiter: ",",
		encoding:       DefaultEncoding,
		nullFormat:     "\\N",
		hdfsUsername:   DefaultUsername,
		factory:        factory.GetGlobalFactory(),
	}
}

func (job *HdfsWriterJob) Init(config config.Configuration) error {
	job.config = config

	// 获取必需参数
	job.defaultFS = config.GetString("parameter.defaultFS")
	if job.defaultFS == "" {
		return fmt.Errorf("defaultFS is required")
	}

	job.path = config.GetString("parameter.path")
	if job.path == "" {
		return fmt.Errorf("path is required")
	}
	if !strings.HasPrefix(job.path, "/") {
		return fmt.Errorf("path must be absolute path: %s", job.path)
	}
	if strings.Contains(job.path, "*") || strings.Contains(job.path, "?") {
		return fmt.Errorf("path cannot contain wildcards: %s", job.path)
	}

	job.fileName = config.GetString("parameter.fileName")
	if job.fileName == "" {
		return fmt.Errorf("fileName is required")
	}

	job.fileType = strings.ToUpper(config.GetStringWithDefault("parameter.fileType", FileTypeText))
	if !job.isValidFileType(job.fileType) {
		return fmt.Errorf("unsupported fileType: %s, supported types: TEXT, ORC, PARQUET", job.fileType)
	}

	// 解析列配置
	if err := job.parseColumns(); err != nil {
		return err
	}

	// 获取写模式
	job.writeMode = strings.ToLower(strings.TrimSpace(config.GetStringWithDefault("parameter.writeMode", WriteModeAppend)))
	if !job.isValidWriteMode(job.writeMode) {
		return fmt.Errorf("unsupported writeMode: %s, supported modes: append, nonconflict, truncate", job.writeMode)
	}

	// 获取字段分隔符
	job.fieldDelimiter = config.GetString("parameter.fieldDelimiter")
	if job.fieldDelimiter == "" {
		return fmt.Errorf("fieldDelimiter is required")
	}
	if len(job.fieldDelimiter) != 1 {
		return fmt.Errorf("fieldDelimiter must be a single character, got: %s", job.fieldDelimiter)
	}

	// 获取其他配置
	job.encoding = config.GetStringWithDefault("parameter.encoding", DefaultEncoding)
	job.compress = config.GetString("parameter.compress")
	job.nullFormat = config.GetStringWithDefault("parameter.nullFormat", "\\N")

	// 验证压缩配置
	if err := job.validateCompression(); err != nil {
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
	job.hdfsUsername = config.GetStringWithDefault("parameter.hdfsUsername", DefaultUsername)

	// 获取Hadoop配置
	if hadoopConfig := config.GetMap("parameter.hadoopConfig"); hadoopConfig != nil {
		job.hadoopConfig = hadoopConfig
	}

	log.Printf("HdfsWriter initialized: defaultFS=%s, path=%s, fileType=%s, columns=%d",
		job.defaultFS, job.path, job.fileType, len(job.columns))

	return nil
}

func (job *HdfsWriterJob) parseColumns() error {
	columnsConfig := job.config.GetList("parameter.column")
	if len(columnsConfig) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	for _, columnConfig := range columnsConfig {
		if column, ok := columnConfig.(map[string]interface{}); ok {
			// 验证name和type是必需的
			name, hasName := column["name"]
			columnType, hasType := column["type"]

			if !hasName || name == "" {
				return fmt.Errorf("column name is required")
			}
			if !hasType || columnType == "" {
				return fmt.Errorf("column type is required")
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

func (job *HdfsWriterJob) isValidFileType(fileType string) bool {
	validTypes := []string{FileTypeText, FileTypeORC, FileTypeParquet}
	for _, vt := range validTypes {
		if vt == fileType {
			return true
		}
	}
	return false
}

func (job *HdfsWriterJob) isValidWriteMode(writeMode string) bool {
	validModes := []string{WriteModeAppend, WriteModeNonConflict, WriteModeTruncate}
	for _, vm := range validModes {
		if vm == writeMode {
			return true
		}
	}
	return false
}

func (job *HdfsWriterJob) validateCompression() error {
	if job.compress == "" {
		return nil
	}

	compress := strings.ToUpper(strings.TrimSpace(job.compress))

	if job.fileType == FileTypeText {
		validCompressTypes := []string{"GZIP", "BZIP2"}
		for _, vct := range validCompressTypes {
			if vct == compress {
				job.compress = compress
				return nil
			}
		}
		return fmt.Errorf("TEXT file only supports GZIP, BZIP2 compression, got: %s", job.compress)
	} else if job.fileType == FileTypeORC {
		validCompressTypes := []string{"NONE", "SNAPPY"}
		if compress == "" {
			job.compress = "NONE"
			return nil
		}
		for _, vct := range validCompressTypes {
			if vct == compress {
				job.compress = compress
				return nil
			}
		}
		return fmt.Errorf("ORC file only supports SNAPPY compression, got: %s", job.compress)
	}

	return nil
}

func (job *HdfsWriterJob) Prepare() error {
	client, err := job.createHdfsClient()
	if err != nil {
		return fmt.Errorf("failed to create HDFS client: %v", err)
	}
	defer client.Close()

	// 检查路径是否存在
	if exists, err := job.pathExists(client, job.path); err != nil {
		return fmt.Errorf("failed to check path existence: %v", err)
	} else if exists {
		// 路径存在，检查是否是目录
		if isDir, err := job.isDirectory(client, job.path); err != nil {
			return fmt.Errorf("failed to check if path is directory: %v", err)
		} else if !isDir {
			return fmt.Errorf("path is not a directory: %s", job.path)
		}

		// 根据写模式处理现有文件
		if err := job.handleExistingFiles(client); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("path does not exist: %s, please create the corresponding database and table in Hive first", job.path)
	}

	return nil
}

func (job *HdfsWriterJob) createHdfsClient() (*hdfs.Client, error) {
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

func (job *HdfsWriterJob) pathExists(client *hdfs.Client, path string) (bool, error) {
	_, err := client.Stat(path)
	if err != nil {
		if strings.Contains(err.Error(), "file does not exist") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (job *HdfsWriterJob) isDirectory(client *hdfs.Client, path string) (bool, error) {
	fileInfo, err := client.Stat(path)
	if err != nil {
		return false, err
	}
	return fileInfo.IsDir(), nil
}

func (job *HdfsWriterJob) handleExistingFiles(client *hdfs.Client) error {
	// 列出目录下以fileName开头的文件
	files, err := client.ReadDir(job.path)
	if err != nil {
		return fmt.Errorf("failed to read directory: %v", err)
	}

	existingFiles := make([]string, 0)
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), job.fileName) {
			existingFiles = append(existingFiles, filepath.Join(job.path, file.Name()))
		}
	}

	isExistFile := len(existingFiles) > 0

	switch job.writeMode {
	case WriteModeAppend:
		log.Printf("Write mode is append, no cleanup needed for path: %s", job.path)

	case WriteModeNonConflict:
		if isExistFile {
			return fmt.Errorf("write mode is nonconflict, but path %s is not empty, existing files: %v", job.path, existingFiles)
		}
		log.Printf("Write mode is nonconflict, path is empty: %s", job.path)

	case WriteModeTruncate:
		if isExistFile {
			log.Printf("Write mode is truncate, deleting existing files in path: %s", job.path)
			for _, file := range existingFiles {
				if err := client.Remove(file); err != nil {
					return fmt.Errorf("failed to delete file %s: %v", file, err)
				}
			}
		}
	}

	return nil
}

func (job *HdfsWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	writerConfigs := make([]config.Configuration, 0)

	client, err := job.createHdfsClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create HDFS client: %v", err)
	}
	defer client.Close()

	// 获取现有文件列表以避免冲突
	allFiles := make(map[string]bool)
	if exists, _ := job.pathExists(client, job.path); exists {
		files, err := client.ReadDir(job.path)
		if err == nil {
			for _, file := range files {
				fullPath := fmt.Sprintf("%s%s%s", job.defaultFS, job.path, file.Name())
				allFiles[fullPath] = true
			}
		}
	}

	// 创建临时存储路径和最终存储路径
	storePath := job.buildTmpFilePath(job.path)
	endStorePath := job.buildFilePath()

	for i := 0; i < mandatoryNumber; i++ {
		taskConfig := job.config.Clone()

		// 生成唯一的文件后缀
		fileSuffix := job.generateUniqueSuffix()

		// 根据文件类型和压缩设置确定文件扩展名
		if job.fileType == FileTypeParquet {
			if job.compress != "" && job.compress != "NONE" {
				fileSuffix += "." + strings.ToLower(job.compress)
			}
			fileSuffix += ".parquet"
		}

		// 构建完整的文件名
		fullFileName := fmt.Sprintf("%s%s%s__%s", job.defaultFS, storePath, job.fileName, fileSuffix)
		endFullFileName := fmt.Sprintf("%s%s%s__%s", job.defaultFS, endStorePath, job.fileName, fileSuffix)

		// 确保文件名不冲突
		for allFiles[endFullFileName] {
			fileSuffix = job.generateUniqueSuffix()
			if job.fileType == FileTypeParquet {
				if job.compress != "" && job.compress != "NONE" {
					fileSuffix += "." + strings.ToLower(job.compress)
				}
				fileSuffix += ".parquet"
			}
			fullFileName = fmt.Sprintf("%s%s%s__%s", job.defaultFS, storePath, job.fileName, fileSuffix)
			endFullFileName = fmt.Sprintf("%s%s%s__%s", job.defaultFS, endStorePath, job.fileName, fileSuffix)
		}

		allFiles[endFullFileName] = true

		// 根据压缩类型调整文件名
		tmpFileName := fullFileName
		endFileName := endFullFileName
		if job.compress == "GZIP" {
			tmpFileName += ".gz"
			endFileName += ".gz"
		} else if job.compress == "BZIP2" {
			tmpFileName += ".bz2"
			endFileName += ".bz2"
		}

		job.tmpFiles = append(job.tmpFiles, tmpFileName)
		job.endFiles = append(job.endFiles, endFileName)

		// 设置任务配置
		taskConfig.Set("fileName", fullFileName)
		taskConfig.Set("taskId", i)

		log.Printf("Split task %d: file=%s", i, fullFileName)
		writerConfigs = append(writerConfigs, taskConfig)
	}

	log.Printf("Split into %d writer tasks", len(writerConfigs))
	return writerConfigs, nil
}

func (job *HdfsWriterJob) buildFilePath() string {
	path := job.path
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return path
}

func (job *HdfsWriterJob) buildTmpFilePath(userPath string) string {
	// 创建临时目录路径
	tmpSuffix := job.generateUniqueSuffix()

	if strings.HasSuffix(userPath, "/") {
		if userPath == "/" {
			return fmt.Sprintf("/__%s/", tmpSuffix)
		}
		return fmt.Sprintf("%s__%s/", strings.TrimSuffix(userPath, "/"), tmpSuffix)
	}

	return fmt.Sprintf("%s__%s/", userPath, tmpSuffix)
}

func (job *HdfsWriterJob) generateUniqueSuffix() string {
	// 生成UUID样式的后缀
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x_%x_%x_%x_%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (job *HdfsWriterJob) Post() error {
	// 将临时文件重命名为最终文件
	if len(job.tmpFiles) != len(job.endFiles) {
		return fmt.Errorf("tmp files count (%d) does not match end files count (%d)",
			len(job.tmpFiles), len(job.endFiles))
	}

	client, err := job.createHdfsClient()
	if err != nil {
		return fmt.Errorf("failed to create HDFS client: %v", err)
	}
	defer client.Close()

	for i, tmpFile := range job.tmpFiles {
		endFile := job.endFiles[i]

		// 从完整路径中提取HDFS路径
		tmpPath := strings.TrimPrefix(tmpFile, job.defaultFS)
		endPath := strings.TrimPrefix(endFile, job.defaultFS)

		if err := client.Rename(tmpPath, endPath); err != nil {
			return fmt.Errorf("failed to rename %s to %s: %v", tmpPath, endPath, err)
		}
		log.Printf("Renamed %s to %s", tmpPath, endPath)
	}

	return nil
}

func (job *HdfsWriterJob) Destroy() error {
	// 清理资源
	return nil
}

// HdfsWriterTask HDFS文件写入任务
type HdfsWriterTask struct {
	config     config.Configuration
	writerJob  *HdfsWriterJob
	defaultFS  string
	fileType   string
	fileName   string
	factory    *factory.DataXFactory
}

func NewHdfsWriterTask() *HdfsWriterTask {
	return &HdfsWriterTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *HdfsWriterTask) Init(config config.Configuration) error {
	task.config = config

	// 创建WriterJob来重用配置逻辑
	task.writerJob = NewHdfsWriterJob()
	err := task.writerJob.Init(config)
	if err != nil {
		return err
	}

	// 获取任务特定配置
	task.defaultFS = config.GetString("parameter.defaultFS")
	task.fileType = config.GetString("parameter.fileType")
	task.fileName = config.GetString("fileName")

	if task.fileName == "" {
		return fmt.Errorf("fileName not provided for task")
	}

	log.Printf("HdfsWriter task initialized: fileName=%s, fileType=%s", task.fileName, task.fileType)
	return nil
}

func (task *HdfsWriterTask) Prepare() error {
	return nil
}

func (task *HdfsWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	log.Printf("Starting write to file: %s", task.fileName)

	client, err := task.writerJob.createHdfsClient()
	if err != nil {
		return fmt.Errorf("failed to create HDFS client: %v", err)
	}
	defer client.Close()

	// 根据文件类型选择不同的写入方式
	switch task.fileType {
	case FileTypeText:
		return task.writeTextFile(client, recordReceiver)
	case FileTypeORC:
		return fmt.Errorf("ORC format writing not yet implemented")
	case FileTypeParquet:
		return fmt.Errorf("Parquet format writing not yet implemented")
	default:
		return fmt.Errorf("unsupported file type: %s", task.fileType)
	}
}

func (task *HdfsWriterTask) writeTextFile(client *hdfs.Client, recordReceiver plugin.RecordReceiver) error {
	// 从完整路径中提取HDFS路径
	filePath := strings.TrimPrefix(task.fileName, task.defaultFS)

	// 确保目录存在
	dir := filepath.Dir(filePath)
	if err := client.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", dir, err)
	}

	// 创建文件
	writer, err := client.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %v", filePath, err)
	}
	defer writer.Close()

	// 创建带缓冲的写入器
	bufferedWriter := bufio.NewWriter(writer)
	defer bufferedWriter.Flush()

	recordCount := int64(0)

	// 读取记录并写入文件
	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err.Error() == "no more records" {
				break
			}
			return fmt.Errorf("failed to get record: %v", err)
		}
		if record == nil {
			break
		}

		// 将记录转换为文本行
		line, err := task.recordToLine(record)
		if err != nil {
			return fmt.Errorf("failed to convert record to line: %v", err)
		}

		// 写入行
		if _, err := bufferedWriter.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("failed to write line: %v", err)
		}

		recordCount++

		// 每1000条记录输出一次进度
		if recordCount%1000 == 0 {
			log.Printf("Written %d records to %s", recordCount, filePath)
		}
	}

	log.Printf("Completed writing %d records to %s", recordCount, filePath)
	return nil
}

func (task *HdfsWriterTask) recordToLine(record element.Record) (string, error) {
	columns := make([]string, 0)

	// 遍历记录中的所有列
	for i := 0; i < record.GetColumnNumber(); i++ {
		column := record.GetColumn(i)

		var valueStr string
		if column == nil {
			valueStr = task.writerJob.nullFormat
		} else {
			switch column.GetType() {
			case element.TypeString:
				if column.GetAsString() == "" {
					valueStr = task.writerJob.nullFormat
				} else {
					valueStr = column.GetAsString()
				}
			case element.TypeLong:
				if longVal, err := column.GetAsLong(); err == nil {
					valueStr = fmt.Sprintf("%d", longVal)
				} else {
					valueStr = task.writerJob.nullFormat
				}
			case element.TypeDouble:
				if doubleVal, err := column.GetAsDouble(); err == nil {
					valueStr = fmt.Sprintf("%f", doubleVal)
				} else {
					valueStr = task.writerJob.nullFormat
				}
			case element.TypeBool:
				if boolVal, err := column.GetAsBool(); err == nil {
					valueStr = fmt.Sprintf("%t", boolVal)
				} else {
					valueStr = task.writerJob.nullFormat
				}
			case element.TypeDate:
				if dateVal, err := column.GetAsDate(); err == nil {
					valueStr = dateVal.Format("2006-01-02 15:04:05")
				} else {
					valueStr = task.writerJob.nullFormat
				}
			case element.TypeBytes:
				if bytesVal, err := column.GetAsBytes(); err == nil {
					valueStr = string(bytesVal)
				} else {
					valueStr = task.writerJob.nullFormat
				}
			default:
				valueStr = column.GetAsString()
			}
		}

		columns = append(columns, valueStr)
	}

	return strings.Join(columns, task.writerJob.fieldDelimiter), nil
}

func (task *HdfsWriterTask) Post() error {
	return nil
}

func (task *HdfsWriterTask) Destroy() error {
	return nil
}