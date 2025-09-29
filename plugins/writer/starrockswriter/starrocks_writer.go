package starrockswriter

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/factory"
	coreplugin "github.com/longkeyy/go-datax/core/registry"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
	"github.com/google/uuid"
)

const (
	DefaultBatchSize     = 500000
	DefaultBatchBytes    = 5 * 1024 * 1024 // 5MB
	DefaultFlushInterval = 300000           // 300 seconds
	DefaultMaxRetries    = 1
)

// StreamLoadFormat Stream Load数据格式
type StreamLoadFormat string

const (
	StreamLoadFormatCSV  StreamLoadFormat = "CSV"
	StreamLoadFormatJSON StreamLoadFormat = "JSON"
)

// StarRocksWriterJob StarRocks写入作业
// 支持通过Stream Load API进行高效批量写入
type StarRocksWriterJob struct {
	config       config.Configuration
	username     string
	password     string
	jdbcUrls     []string
	loadUrls     []string // Stream Load HTTP接口地址
	database     string
	tables       []string
	columns      []string
	preSql       []string
	postSql      []string
	writeMode    string
	batchSize    int
	maxBatchSize int64
	flushInterval int64
	maxRetries   int
	labelPrefix  string
	loadProps    map[string]interface{}
	sessionConf  []string
	factory      *factory.DataXFactory
}

func NewStarRocksWriterJob() *StarRocksWriterJob {
	return &StarRocksWriterJob{
		factory: factory.GetGlobalFactory(),
	}
}

func (job *StarRocksWriterJob) Init(config config.Configuration) error {
	job.config = config

	// 获取数据库连接参数
	job.username = config.GetString("parameter.username")
	job.password = config.GetString("parameter.password")

	if job.username == "" || job.password == "" {
		return fmt.Errorf("username and password are required")
	}

	// 获取连接信息
	connections := config.GetListConfiguration("parameter.connection")
	if len(connections) == 0 {
		return fmt.Errorf("connection configuration is required")
	}

	// 处理第一个连接配置
	conn := connections[0]
	job.jdbcUrls = conn.GetStringList("jdbcUrl")
	job.tables = conn.GetStringList("table")

	if len(job.jdbcUrls) == 0 || len(job.tables) == 0 {
		return fmt.Errorf("jdbcUrl and table are required")
	}

	// 获取数据库名
	job.database = config.GetString("parameter.database")
	if job.database == "" {
		// 从JDBC URL中提取数据库名
		if db, err := job.extractDatabaseFromJdbcUrl(job.jdbcUrls[0]); err == nil {
			job.database = db
		} else {
			return fmt.Errorf("database is required")
		}
	}

	// 获取Stream Load URL列表
	job.loadUrls = config.GetStringList("parameter.loadUrl")
	if len(job.loadUrls) == 0 {
		return fmt.Errorf("loadUrl is required for StarRocks stream load")
	}

	// 获取列信息
	job.columns = config.GetStringList("parameter.column")
	if len(job.columns) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	// 如果配置为*，需要获取表的实际列名
	if len(job.columns) == 1 && job.columns[0] == "*" {
		actualColumns, err := job.getTableColumns()
		if err != nil {
			logger.Component().WithComponent("StarRocksWriter").Warn("Failed to get table columns, will use * as is",
				zap.Error(err))
		} else {
			job.columns = actualColumns
		}
	}

	// 获取可选参数
	job.preSql = config.GetStringList("parameter.preSql")
	job.postSql = config.GetStringList("parameter.postSql")
	job.writeMode = config.GetString("parameter.writeMode")
	if job.writeMode == "" {
		job.writeMode = "insert"
	}

	// 批量处理参数
	job.batchSize = config.GetInt("parameter.maxBatchRows")
	if job.batchSize <= 0 {
		job.batchSize = DefaultBatchSize
	}

	if maxBatchSizeInt := config.GetInt("parameter.maxBatchSize"); maxBatchSizeInt > 0 {
		job.maxBatchSize = int64(maxBatchSizeInt)
	} else {
		job.maxBatchSize = DefaultBatchBytes
	}

	if flushIntervalInt := config.GetInt("parameter.flushInterval"); flushIntervalInt > 0 {
		job.flushInterval = int64(flushIntervalInt)
	} else {
		job.flushInterval = DefaultFlushInterval
	}

	job.maxRetries = config.GetInt("parameter.maxRetries")
	if job.maxRetries <= 0 {
		job.maxRetries = DefaultMaxRetries
	}

	// Stream Load相关参数
	job.labelPrefix = config.GetString("parameter.labelPrefix")
	job.loadProps = config.GetMap("parameter.loadProps")
	if job.loadProps == nil {
		job.loadProps = make(map[string]interface{})
	}

	// 获取session配置
	job.sessionConf = config.GetStringList("parameter.session")

	logger.Component().WithComponent("StarRocksWriter").Info("StarRocks Writer initialized",
		zap.String("database", job.database),
		zap.Any("tables", job.tables),
		zap.Any("columns", job.columns),
		zap.Any("loadUrls", job.loadUrls),
		zap.String("writeMode", job.writeMode),
		zap.Int("batchSize", job.batchSize),
		zap.Int64("maxBatchSize", job.maxBatchSize))
	return nil
}

func (job *StarRocksWriterJob) extractDatabaseFromJdbcUrl(jdbcUrl string) (string, error) {
	// 解析JDBC URL: jdbc:mysql://host:port/database?参数
	if !strings.HasPrefix(jdbcUrl, "jdbc:mysql://") {
		return "", fmt.Errorf("invalid StarRocks JDBC URL")
	}

	url := strings.TrimPrefix(jdbcUrl, "jdbc:mysql://")
	parts := strings.Split(url, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid JDBC URL format")
	}

	dbAndParams := strings.Join(parts[1:], "/")
	if idx := strings.Index(dbAndParams, "?"); idx != -1 {
		return dbAndParams[:idx], nil
	}
	return dbAndParams, nil
}

func (job *StarRocksWriterJob) Prepare() error {
	// 执行pre SQL语句
	if len(job.preSql) > 0 {
		db, err := job.connect()
		if err != nil {
			return fmt.Errorf("failed to connect for pre SQL: %v", err)
		}
		defer func() {
			if sqlDB, err := db.DB(); err == nil {
				sqlDB.Close()
			}
		}()

		for _, sql := range job.preSql {
			logger.Component().WithComponent("StarRocksWriter").Info("Executing pre SQL",
				zap.String("sql", sql))
			if err := db.Exec(sql).Error; err != nil {
				return fmt.Errorf("failed to execute pre SQL: %v", err)
			}
		}
	}
	return nil
}

func (job *StarRocksWriterJob) getTableColumns() ([]string, error) {
	db, err := job.connect()
	if err != nil {
		return nil, err
	}
	defer func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	// 查询表的列信息
	query := `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position`

	rows, err := db.Raw(query, job.database, job.tables[0]).Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to query table columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan column name: %v", err)
		}
		columns = append(columns, columnName)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %s", job.tables[0])
	}

	logger.Component().WithComponent("StarRocksWriter").Info("Retrieved table columns",
		zap.String("table", job.tables[0]),
		zap.Any("columns", columns))
	return columns, nil
}

func (job *StarRocksWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	taskConfigs := make([]config.Configuration, 0)

	// StarRocks Writer通常不需要分片，每个task写入相同的表
	for i := 0; i < mandatoryNumber; i++ {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	logger.Component().WithComponent("StarRocksWriter").Info("Split into StarRocks writer tasks",
		zap.Int("taskCount", len(taskConfigs)))
	return taskConfigs, nil
}

func (job *StarRocksWriterJob) connect() (*gorm.DB, error) {
	// 构建连接字符串
	jdbcUrl := job.jdbcUrls[0]

	// 转换JDBC URL为MySQL连接字符串
	dsn, err := job.convertJdbcUrl(jdbcUrl)
	if err != nil {
		return nil, err
	}

	// 连接数据库（StarRocks兼容MySQL协议）
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: gormLogger.Default.LogMode(gormLogger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to StarRocks database: %v", err)
	}

	// 应用session配置
	if len(job.sessionConf) > 0 {
		for _, sessionSQL := range job.sessionConf {
			if err := db.Exec(sessionSQL).Error; err != nil {
				logger.Component().WithComponent("StarRocksWriter").Warn("Failed to execute session SQL",
				zap.String("sql", sessionSQL),
				zap.Error(err))
			}
		}
	}

	return db, nil
}

func (job *StarRocksWriterJob) convertJdbcUrl(jdbcUrl string) (string, error) {
	// 解析JDBC URL: jdbc:mysql://host:port/database?参数
	if !strings.HasPrefix(jdbcUrl, "jdbc:mysql://") {
		return "", fmt.Errorf("invalid StarRocks JDBC URL (should use mysql protocol): %s", jdbcUrl)
	}

	// 移除jdbc:mysql://前缀
	url := strings.TrimPrefix(jdbcUrl, "jdbc:mysql://")

	// 分离host:port和database以及参数
	var hostPort, database, params string
	parts := strings.Split(url, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid JDBC URL format: %s", jdbcUrl)
	}

	hostPort = parts[0]
	dbAndParams := strings.Join(parts[1:], "/")

	// 分离数据库名和参数
	if idx := strings.Index(dbAndParams, "?"); idx != -1 {
		database = dbAndParams[:idx]
		params = dbAndParams[idx+1:]
	} else {
		database = dbAndParams
	}

	// 构建MySQL DSN字符串，优化StarRocks连接参数
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&loc=Local&charset=utf8mb4",
		job.username,
		job.password,
		hostPort,
		database)

	// 添加用户配置的额外参数
	if params != "" {
		dsn += "&" + params
	}

	return dsn, nil
}

func (job *StarRocksWriterJob) Post() error {
	// 执行post SQL语句
	if len(job.postSql) > 0 {
		db, err := job.connect()
		if err != nil {
			return fmt.Errorf("failed to connect for post SQL: %v", err)
		}
		defer func() {
			if sqlDB, err := db.DB(); err == nil {
				sqlDB.Close()
			}
		}()

		for _, sql := range job.postSql {
			logger.Component().WithComponent("StarRocksWriter").Info("Executing post SQL",
				zap.String("sql", sql))
			if err := db.Exec(sql).Error; err != nil {
				return fmt.Errorf("failed to execute post SQL: %v", err)
			}
		}
	}
	return nil
}

func (job *StarRocksWriterJob) Destroy() error {
	return nil
}

// StarRocksWriterTask StarRocks写入任务
type StarRocksWriterTask struct {
	config         config.Configuration
	writerJob      *StarRocksWriterJob
	db             *gorm.DB
	streamLoader   *StreamLoader
	records        []element.Record
	factory        *factory.DataXFactory
}

func NewStarRocksWriterTask() *StarRocksWriterTask {
	return &StarRocksWriterTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *StarRocksWriterTask) Init(config config.Configuration) error {
	task.config = config

	// 创建WriterJob来重用连接逻辑
	task.writerJob = NewStarRocksWriterJob()
	err := task.writerJob.Init(config)
	if err != nil {
		return err
	}

	// 建立数据库连接
	task.db, err = task.writerJob.connect()
	if err != nil {
		return fmt.Errorf("failed to connect to StarRocks database: %v", err)
	}

	// 初始化Stream Loader
	task.streamLoader = NewStreamLoader(task.writerJob)

	// 初始化记录缓冲区
	task.records = make([]element.Record, 0, task.writerJob.batchSize)

	return nil
}

func (task *StarRocksWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	defer func() {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
		if task.streamLoader != nil {
			task.streamLoader.Close()
		}
	}()

	recordCount := 0
	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == coreplugin.ErrChannelClosed {
				break
			}
			return fmt.Errorf("failed to get record: %v", err)
		}

		// 检查列数匹配
		if record.GetColumnNumber() != len(task.writerJob.columns) {
			return fmt.Errorf("column count mismatch: expected %d, got %d",
				len(task.writerJob.columns), record.GetColumnNumber())
		}

		// 将记录转换为StarRocks格式并发送到Stream Loader
		recordData := task.serializeRecord(record)
		if err := task.streamLoader.WriteRecord(recordData); err != nil {
			return fmt.Errorf("failed to write record to stream loader: %v", err)
		}

		recordCount++
		if recordCount%1000 == 0 {
			logger.Component().WithComponent("StarRocksWriter").Info("Writing progress",
				zap.Int("recordCount", recordCount))
		}
	}

	logger.Component().WithComponent("StarRocksWriter").Info("Writing completed",
		zap.Int("totalRecords", recordCount))
	return nil
}

// serializeRecord 将记录序列化为CSV或JSON格式
func (task *StarRocksWriterTask) serializeRecord(record element.Record) string {
	format := task.getStreamLoadFormat()

	if format == StreamLoadFormatJSON {
		return task.serializeToJSON(record)
	}

	// 默认使用CSV格式
	return task.serializeToCSV(record)
}

func (task *StarRocksWriterTask) getStreamLoadFormat() StreamLoadFormat {
	if task.writerJob.loadProps != nil {
		if formatVal, exists := task.writerJob.loadProps["format"]; exists {
			if formatStr, ok := formatVal.(string); ok && strings.ToUpper(formatStr) == "JSON" {
				return StreamLoadFormatJSON
			}
		}
	}
	return StreamLoadFormatCSV
}

func (task *StarRocksWriterTask) serializeToCSV(record element.Record) string {
	delimiter := task.getColumnDelimiter()
	values := make([]string, record.GetColumnNumber())

	for i := 0; i < record.GetColumnNumber(); i++ {
		column := record.GetColumn(i)
		if column == nil {
			values[i] = "\\N" // StarRocks NULL值表示
		} else {
			value := task.convertColumnValue(column)
			// CSV格式需要转义特殊字符
			values[i] = task.escapeCSVValue(fmt.Sprintf("%v", value), delimiter)
		}
	}

	return strings.Join(values, delimiter)
}

func (task *StarRocksWriterTask) serializeToJSON(record element.Record) string {
	data := make(map[string]interface{})

	for i := 0; i < record.GetColumnNumber() && i < len(task.writerJob.columns); i++ {
		column := record.GetColumn(i)
		columnName := task.writerJob.columns[i]

		if column == nil {
			data[columnName] = nil
		} else {
			data[columnName] = task.convertColumnValue(column)
		}
	}

	jsonBytes, _ := json.Marshal(data)
	return string(jsonBytes)
}

func (task *StarRocksWriterTask) getColumnDelimiter() string {
	if task.writerJob.loadProps != nil {
		if delimiter, exists := task.writerJob.loadProps["column_separator"]; exists {
			if delimiterStr, ok := delimiter.(string); ok {
				return delimiterStr
			}
		}
	}
	return "\t" // 默认使用制表符
}

func (task *StarRocksWriterTask) escapeCSVValue(value, delimiter string) string {
	// 如果包含分隔符、换行符或引号，需要用引号包围并转义
	if strings.Contains(value, delimiter) || strings.Contains(value, "\n") || strings.Contains(value, "\"") {
		escaped := strings.ReplaceAll(value, "\"", "\"\"")
		return "\"" + escaped + "\""
	}
	return value
}

func (task *StarRocksWriterTask) convertColumnValue(column element.Column) interface{} {
	if column == nil {
		return nil
	}

	// 使用新的Column接口方法
	switch column.GetType() {
	case element.TypeString:
		return column.GetAsString()
	case element.TypeLong:
		val, _ := column.GetAsLong()
		return val
	case element.TypeDouble:
		val, _ := column.GetAsDouble()
		return val
	case element.TypeDate:
		val, _ := column.GetAsDate()
		return val.Format("2006-01-02 15:04:05")
	case element.TypeBool:
		val, _ := column.GetAsBool()
		return val
	case element.TypeBytes:
		val, _ := column.GetAsBytes()
		return string(val)
	default:
		// 其他类型作为字符串处理
		return column.GetAsString()
	}
}

func (task *StarRocksWriterTask) Prepare() error {
	return nil
}

func (task *StarRocksWriterTask) Post() error {
	return nil
}

func (task *StarRocksWriterTask) Destroy() error {
	if task.db != nil {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}
	if task.streamLoader != nil {
		task.streamLoader.Close()
	}
	return nil
}

// StreamLoader 实现StarRocks Stream Load功能
type StreamLoader struct {
	options       *StarRocksWriterJob
	buffer        [][]byte
	batchCount    int
	batchSize     int64
	lastFlushTime time.Time
	closed        bool
	flushException error
	mu            sync.Mutex
	flushTimer    *time.Timer
}

func NewStreamLoader(options *StarRocksWriterJob) *StreamLoader {
	loader := &StreamLoader{
		options:       options,
		buffer:        make([][]byte, 0, options.batchSize),
		lastFlushTime: time.Now(),
	}

	// 启动定时刷新
	loader.startFlushTimer()

	return loader
}

func (sl *StreamLoader) WriteRecord(record string) error {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	if sl.closed {
		return fmt.Errorf("stream loader is closed")
	}

	if sl.flushException != nil {
		return sl.flushException
	}

	// 添加记录到缓冲区
	recordBytes := []byte(record)
	sl.buffer = append(sl.buffer, recordBytes)
	sl.batchCount++
	sl.batchSize += int64(len(recordBytes))

	// 检查是否需要刷新
	if sl.batchCount >= sl.options.batchSize || sl.batchSize >= sl.options.maxBatchSize {
		return sl.flush(false)
	}

	return nil
}

func (sl *StreamLoader) flush(waitComplete bool) error {
	if sl.batchCount == 0 {
		return nil
	}

	label := sl.createLabel()
	data := sl.joinRecords()

	logger.Component().WithComponent("StarRocksWriter").Info("Starting stream load",
		zap.String("label", label),
		zap.Int("records", sl.batchCount),
		zap.Int64("bytes", sl.batchSize))

	// 执行Stream Load
	err := sl.doStreamLoad(label, data)
	if err != nil {
		sl.flushException = err
		return err
	}

	// 清空缓冲区
	sl.buffer = sl.buffer[:0]
	sl.batchCount = 0
	sl.batchSize = 0
	sl.lastFlushTime = time.Now()

	return nil
}

func (sl *StreamLoader) createLabel() string {
	label := ""
	if sl.options.labelPrefix != "" {
		label = sl.options.labelPrefix
	}
	label += uuid.New().String()
	return label
}

func (sl *StreamLoader) joinRecords() []byte {
	format := sl.getStreamLoadFormat()

	if format == StreamLoadFormatJSON {
		return sl.joinRecordsAsJSON()
	}

	return sl.joinRecordsAsCSV()
}

func (sl *StreamLoader) getStreamLoadFormat() StreamLoadFormat {
	if sl.options.loadProps != nil {
		if formatVal, exists := sl.options.loadProps["format"]; exists {
			if formatStr, ok := formatVal.(string); ok && strings.ToUpper(formatStr) == "JSON" {
				return StreamLoadFormatJSON
			}
		}
	}
	return StreamLoadFormatCSV
}

func (sl *StreamLoader) joinRecordsAsCSV() []byte {
	rowDelimiter := sl.getRowDelimiter()
	rowDelimiterBytes := []byte(rowDelimiter)

	// 计算总大小
	totalSize := sl.batchSize + int64(len(sl.buffer)*len(rowDelimiterBytes))

	// 构建数据
	result := make([]byte, 0, totalSize)
	for _, row := range sl.buffer {
		result = append(result, row...)
		result = append(result, rowDelimiterBytes...)
	}

	return result
}

func (sl *StreamLoader) joinRecordsAsJSON() []byte {
	result := bytes.NewBuffer(make([]byte, 0, sl.batchSize+int64(len(sl.buffer))+2))

	result.WriteByte('[')
	for i, row := range sl.buffer {
		if i > 0 {
			result.WriteByte(',')
		}
		result.Write(row)
	}
	result.WriteByte(']')

	return result.Bytes()
}

func (sl *StreamLoader) getRowDelimiter() string {
	if sl.options.loadProps != nil {
		if delimiter, exists := sl.options.loadProps["row_delimiter"]; exists {
			if delimiterStr, ok := delimiter.(string); ok {
				return delimiterStr
			}
		}
	}
	return "\n" // 默认使用换行符
}

func (sl *StreamLoader) doStreamLoad(label string, data []byte) error {
	// 获取可用的load URL
	loadUrl, err := sl.getAvailableLoadUrl()
	if err != nil {
		return err
	}

	// 构建Stream Load URL
	streamLoadUrl := fmt.Sprintf("%s/api/%s/%s/_stream_load",
		loadUrl, sl.options.database, sl.options.tables[0])

	// 重试逻辑
	for i := 0; i <= sl.options.maxRetries; i++ {
		err := sl.executeStreamLoad(streamLoadUrl, label, data)
		if err == nil {
			logger.Component().WithComponent("StarRocksWriter").Info("Stream load completed",
				zap.String("label", label))
			return nil
		}

		logger.Component().WithComponent("StarRocksWriter").Warn("Stream load failed, retrying",
			zap.String("label", label),
			zap.Int("retry", i),
			zap.Error(err))

		if i < sl.options.maxRetries {
			time.Sleep(time.Duration(min(i+1, 10)) * time.Second)
		}
	}

	return fmt.Errorf("stream load failed after %d retries", sl.options.maxRetries)
}

func (sl *StreamLoader) getAvailableLoadUrl() (string, error) {
	for _, url := range sl.options.loadUrls {
		if sl.testConnection("http://" + url) {
			return "http://" + url, nil
		}
	}
	return "", fmt.Errorf("no available load URL found")
}

func (sl *StreamLoader) testConnection(url string) bool {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return true
}

func (sl *StreamLoader) executeStreamLoad(url, label string, data []byte) error {
	client := &http.Client{Timeout: 300 * time.Second}

	req, err := http.NewRequest("PUT", url, bytes.NewReader(data))
	if err != nil {
		return err
	}

	// 设置认证头
	auth := base64.StdEncoding.EncodeToString([]byte(sl.options.username + ":" + sl.options.password))
	req.Header.Set("Authorization", "Basic "+auth)

	// 设置Stream Load参数
	req.Header.Set("label", label)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Expect", "100-continue")

	// 设置列信息（CSV格式）
	if sl.getStreamLoadFormat() == StreamLoadFormatCSV && len(sl.options.columns) > 0 {
		columns := make([]string, len(sl.options.columns))
		for i, col := range sl.options.columns {
			columns[i] = "`" + col + "`"
		}
		req.Header.Set("columns", strings.Join(columns, ","))
	}

	// 设置用户自定义的load属性
	if sl.options.loadProps != nil {
		for key, value := range sl.options.loadProps {
			req.Header.Set(key, fmt.Sprintf("%v", value))
		}
	}

	// 执行请求
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 读取响应
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("stream load failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	// 解析响应
	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("failed to parse stream load response: %v", err)
	}

	// 检查结果状态
	status, ok := result["Status"].(string)
	if !ok {
		return fmt.Errorf("missing Status in stream load response")
	}

	switch status {
	case "Success":
		return nil
	case "Fail":
		message := "unknown error"
		if msg, exists := result["Message"]; exists {
			message = fmt.Sprintf("%v", msg)
		}
		return fmt.Errorf("stream load failed: %s", message)
	case "Label Already Exists":
		// 标签已存在，需要检查状态
		return sl.checkLabelState(label)
	default:
		return fmt.Errorf("unknown stream load status: %s", status)
	}
}

func (sl *StreamLoader) checkLabelState(label string) error {
	// TODO: 实现标签状态检查逻辑
	// 这里简化处理，实际应该查询StarRocks的标签状态API
	logger.Component().WithComponent("StarRocksWriter").Info("Label already exists, assuming success",
		zap.String("label", label))
	return nil
}

func (sl *StreamLoader) startFlushTimer() {
	sl.flushTimer = time.AfterFunc(time.Duration(sl.options.flushInterval)*time.Millisecond, func() {
		sl.mu.Lock()
		defer sl.mu.Unlock()

		if !sl.closed && sl.batchCount > 0 {
			if err := sl.flush(false); err != nil {
				sl.flushException = err
			}
		}

		if !sl.closed {
			sl.startFlushTimer() // 重新启动定时器
		}
	})
}

func (sl *StreamLoader) Close() error {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	if sl.closed {
		return nil
	}

	sl.closed = true

	// 停止定时器
	if sl.flushTimer != nil {
		sl.flushTimer.Stop()
	}

	// 刷新剩余数据
	if sl.batchCount > 0 {
		if err := sl.flush(true); err != nil {
			return err
		}
	}

	return sl.flushException
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}