package doriswriter

import (
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	DefaultMaxBatchRows  = 500000
	DefaultBatchSize     = 104857600 // 100MB
	DefaultFlushInterval = 30000     // 30 seconds
	DefaultMaxRetries    = 3
	DefaultLabelPrefix   = "datax_doris_writer_"
)

// DorisWriterJob Doris写入作业
type DorisWriterJob struct {
	config           *config.Configuration
	loadUrls         []string
	username         string
	password         string
	jdbcUrl          string
	selectedDatabase string
	tables           []string
	columns          []string
	loadProps        map[string]interface{}
	maxBatchRows     int64
	batchSize        int64
	flushInterval    int64
	maxRetries       int
	labelPrefix      string
	preSql           []string
	postSql          []string
}

func NewDorisWriterJob() *DorisWriterJob {
	return &DorisWriterJob{
		maxBatchRows:  DefaultMaxBatchRows,
		batchSize:     DefaultBatchSize,
		flushInterval: DefaultFlushInterval,
		maxRetries:    DefaultMaxRetries,
		labelPrefix:   DefaultLabelPrefix,
		loadProps: map[string]interface{}{
			"format":           "csv",
			"column_separator": "\\t",
			"line_delimiter":   "\\n",
		},
	}
}

func (job *DorisWriterJob) Init(config *config.Configuration) error {
	job.config = config

	// 获取必需参数
	loadUrls := config.GetList("parameter.loadUrl")
	if len(loadUrls) == 0 {
		return fmt.Errorf("loadUrl is required")
	}

	for _, url := range loadUrls {
		if urlStr, ok := url.(string); ok {
			job.loadUrls = append(job.loadUrls, urlStr)
		}
	}

	job.username = config.GetString("parameter.username")
	if job.username == "" {
		return fmt.Errorf("username is required")
	}

	job.password = config.GetString("parameter.password")
	if job.password == "" {
		return fmt.Errorf("password is required")
	}

	// 获取连接配置
	connections := config.GetList("parameter.connection")
	if len(connections) == 0 {
		return fmt.Errorf("connection configuration is required")
	}

	if connMap, ok := connections[0].(map[string]interface{}); ok {
		job.jdbcUrl, _ = connMap["jdbcUrl"].(string)
		job.selectedDatabase, _ = connMap["selectedDatabase"].(string)

		if tables, exists := connMap["table"]; exists {
			if tableList, ok := tables.([]interface{}); ok {
				for _, table := range tableList {
					if tableStr, ok := table.(string); ok {
						job.tables = append(job.tables, tableStr)
					}
				}
			}
		}
	}

	if job.jdbcUrl == "" {
		return fmt.Errorf("jdbcUrl is required")
	}

	if job.selectedDatabase == "" {
		return fmt.Errorf("selectedDatabase is required")
	}

	if len(job.tables) == 0 {
		return fmt.Errorf("table is required")
	}

	// 获取列配置
	columns := config.GetList("parameter.column")
	if len(columns) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	for _, column := range columns {
		if columnStr, ok := column.(string); ok {
			job.columns = append(job.columns, columnStr)
		}
	}

	if len(job.columns) == 0 {
		return fmt.Errorf("no valid columns configured")
	}

	// 获取可选参数
	if loadPropsConfig := config.GetMap("parameter.loadProps"); loadPropsConfig != nil {
		job.loadProps = loadPropsConfig
	}

	job.maxBatchRows = int64(config.GetIntWithDefault("parameter.maxBatchRows", DefaultMaxBatchRows))
	job.batchSize = int64(config.GetIntWithDefault("parameter.batchSize", DefaultBatchSize))
	job.flushInterval = int64(config.GetIntWithDefault("parameter.flushInterval", DefaultFlushInterval))
	job.maxRetries = config.GetIntWithDefault("parameter.maxRetries", DefaultMaxRetries)
	job.labelPrefix = config.GetStringWithDefault("parameter.labelPrefix", DefaultLabelPrefix)

	job.preSql = config.GetStringList("parameter.preSql")
	job.postSql = config.GetStringList("parameter.postSql")

	log.Printf("DorisWriter initialized: loadUrls=%v, database=%s, table=%v, columns=%v",
		job.loadUrls, job.selectedDatabase, job.tables, job.columns)
	return nil
}

func (job *DorisWriterJob) Prepare() error {
	// 执行pre SQL语句
	if len(job.preSql) > 0 {
		err := job.executeSql(job.preSql)
		if err != nil {
			return fmt.Errorf("failed to execute preSql: %v", err)
		}
	}
	return nil
}

func (job *DorisWriterJob) convertJdbcUrl(jdbcUrl string) string {
	// 将JDBC URL转换为MySQL连接字符串
	jdbcUrl = strings.Replace(jdbcUrl, "jdbc:mysql://", "", 1)

	parts := strings.Split(jdbcUrl, "/")
	if len(parts) != 2 {
		return fmt.Sprintf("%s:%s@tcp(%s)/", job.username, job.password, jdbcUrl)
	}

	hostPort := parts[0]
	database := parts[1]

	return fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		job.username, job.password, hostPort, database)
}

func (job *DorisWriterJob) executeSql(sqlList []string) error {
	dsn := job.convertJdbcUrl(job.jdbcUrl)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect to Doris: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	for _, sql := range sqlList {
		log.Printf("Executing SQL: %s", sql)
		err := db.Exec(sql).Error
		if err != nil {
			return fmt.Errorf("failed to execute SQL '%s': %v", sql, err)
		}
	}

	return nil
}

func (job *DorisWriterJob) Split(adviceNumber int) ([]*config.Configuration, error) {
	taskConfigs := make([]*config.Configuration, 0)

	// 创建指定数量的任务配置
	for i := 0; i < adviceNumber; i++ {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	log.Printf("Split into %d tasks", len(taskConfigs))
	return taskConfigs, nil
}

func (job *DorisWriterJob) Post() error {
	// 执行post SQL语句
	if len(job.postSql) > 0 {
		err := job.executeSql(job.postSql)
		if err != nil {
			return fmt.Errorf("failed to execute postSql: %v", err)
		}
	}
	return nil
}

func (job *DorisWriterJob) Destroy() error {
	return nil
}

// DorisWriterTask Doris写入任务
type DorisWriterTask struct {
	config      *config.Configuration
	writerJob   *DorisWriterJob
	buffer      []element.Record
	bufferSize  int64
	httpClient  *http.Client
	currentUrl  int
}

func NewDorisWriterTask() *DorisWriterTask {
	return &DorisWriterTask{
		buffer:     make([]element.Record, 0),
		httpClient: &http.Client{Timeout: 300 * time.Second},
	}
}

func (task *DorisWriterTask) Init(config *config.Configuration) error {
	task.config = config

	// 创建WriterJob来重用配置逻辑
	task.writerJob = NewDorisWriterJob()
	err := task.writerJob.Init(config)
	if err != nil {
		return err
	}

	log.Printf("DorisWriter task initialized")
	return nil
}

func (task *DorisWriterTask) Prepare() error {
	return nil
}

func (task *DorisWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	defer func() {
		// 写入剩余的记录
		if len(task.buffer) > 0 {
			task.flushBuffer()
		}
	}()

	recordCount := int64(0)
	lastFlushTime := time.Now()

	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == plugin.ErrChannelClosed {
				break
			}
			return fmt.Errorf("failed to receive record: %v", err)
		}

		// 添加记录到缓冲区
		task.buffer = append(task.buffer, record)
		task.bufferSize += task.estimateRecordSize(record)

		// 检查是否需要刷新缓冲区
		shouldFlush := false

		// 条件1：达到最大行数
		if int64(len(task.buffer)) >= task.writerJob.maxBatchRows {
			shouldFlush = true
		}

		// 条件2：达到最大字节数
		if task.bufferSize >= task.writerJob.batchSize {
			shouldFlush = true
		}

		// 条件3：达到刷新时间间隔
		if time.Since(lastFlushTime).Milliseconds() >= task.writerJob.flushInterval {
			shouldFlush = true
		}

		if shouldFlush {
			if err := task.flushBuffer(); err != nil {
				return err
			}
			lastFlushTime = time.Now()
		}

		recordCount++

		// 每1000条记录输出一次进度
		if recordCount%1000 == 0 {
			log.Printf("Processed %d records", recordCount)
		}
	}

	log.Printf("Total records processed: %d", recordCount)
	return nil
}

func (task *DorisWriterTask) estimateRecordSize(record element.Record) int64 {
	size := int64(0)
	columnCount := record.GetColumnNumber()

	for i := 0; i < columnCount; i++ {
		column := record.GetColumn(i)
		if column != nil {
			size += int64(len(column.GetAsString()))
		}
	}

	return size + int64(columnCount) // 加上分隔符的大小
}

func (task *DorisWriterTask) flushBuffer() error {
	if len(task.buffer) == 0 {
		return nil
	}

	format, _ := task.writerJob.loadProps["format"].(string)
	if format == "" {
		format = "csv"
	}

	var data []byte
	var contentType string
	var err error

	if format == "json" {
		data, err = task.formatAsJSON()
		contentType = "application/json"
	} else {
		data, err = task.formatAsCSV()
		contentType = "text/plain"
	}

	if err != nil {
		return fmt.Errorf("failed to format data: %v", err)
	}

	// 发送Stream Load请求
	err = task.sendStreamLoad(data, contentType)
	if err != nil {
		return err
	}

	log.Printf("Successfully loaded %d records to Doris", len(task.buffer))

	// 清空缓冲区
	task.buffer = task.buffer[:0]
	task.bufferSize = 0

	return nil
}

func (task *DorisWriterTask) formatAsCSV() ([]byte, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	// 设置分隔符
	separator, _ := task.writerJob.loadProps["column_separator"].(string)
	if separator == "" {
		separator = "\t"
	}
	if separator == "\\t" {
		separator = "\t"
	}
	writer.Comma = rune(separator[0])

	for _, record := range task.buffer {
		row := make([]string, len(task.writerJob.columns))
		columnCount := record.GetColumnNumber()

		for i := 0; i < len(task.writerJob.columns) && i < columnCount; i++ {
			column := record.GetColumn(i)
			if column == nil || column.IsNull() {
				row[i] = "\\N" // Doris的NULL表示
			} else {
				row[i] = column.GetAsString()
			}
		}

		err := writer.Write(row)
		if err != nil {
			return nil, err
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (task *DorisWriterTask) formatAsJSON() ([]byte, error) {
	var records []map[string]interface{}

	for _, record := range task.buffer {
		row := make(map[string]interface{})
		columnCount := record.GetColumnNumber()

		for i := 0; i < len(task.writerJob.columns) && i < columnCount; i++ {
			column := record.GetColumn(i)
			columnName := task.writerJob.columns[i]

			if column == nil || column.IsNull() {
				row[columnName] = nil
			} else {
				row[columnName] = task.convertColumnForJSON(column)
			}
		}

		records = append(records, row)
	}

	return json.Marshal(records)
}

func (task *DorisWriterTask) convertColumnForJSON(column element.Column) interface{} {
	switch column.GetType() {
	case element.TypeLong:
		if val, err := column.GetAsLong(); err == nil {
			return val
		}
	case element.TypeDouble:
		if val, err := column.GetAsDouble(); err == nil {
			return val
		}
	case element.TypeBool:
		if val, err := column.GetAsBool(); err == nil {
			return val
		}
	case element.TypeDate:
		if val, err := column.GetAsDate(); err == nil {
			return val.Format("2006-01-02 15:04:05")
		}
	}
	return column.GetAsString()
}

func (task *DorisWriterTask) sendStreamLoad(data []byte, contentType string) error {
	tableName := task.writerJob.tables[0]
	loadUrl := task.getNextLoadUrl()

	// 生成唯一标签
	label := task.writerJob.labelPrefix + strconv.FormatInt(time.Now().UnixNano(), 10)

	url := fmt.Sprintf("http://%s/api/%s/%s/_stream_load",
		loadUrl, task.writerJob.selectedDatabase, tableName)

	// 创建请求
	req, err := http.NewRequest("PUT", url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// 设置认证
	auth := base64.StdEncoding.EncodeToString([]byte(task.writerJob.username + ":" + task.writerJob.password))
	req.Header.Set("Authorization", "Basic "+auth)

	// 设置Content-Type
	req.Header.Set("Content-Type", contentType)

	// 设置Stream Load相关header
	req.Header.Set("label", label)
	req.Header.Set("Expect", "100-continue")

	// 设置loadProps中的属性作为header
	for key, value := range task.writerJob.loadProps {
		req.Header.Set(key, fmt.Sprintf("%v", value))
	}

	// 重试逻辑
	var lastErr error
	for retry := 0; retry <= task.writerJob.maxRetries; retry++ {
		resp, err := task.httpClient.Do(req)
		if err != nil {
			lastErr = err
			log.Printf("Stream Load attempt %d failed: %v", retry+1, err)
			time.Sleep(time.Duration(retry+1) * time.Second)
			continue
		}

		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode == 200 {
			// 解析响应检查导入状态
			var result map[string]interface{}
			if err := json.Unmarshal(body, &result); err == nil {
				if status, ok := result["Status"].(string); ok && status == "Success" {
					return nil
				} else {
					return fmt.Errorf("stream load failed: %s", string(body))
				}
			}
			return nil
		} else {
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
			log.Printf("Stream Load attempt %d failed: %v", retry+1, lastErr)
			time.Sleep(time.Duration(retry+1) * time.Second)
		}
	}

	return fmt.Errorf("stream load failed after %d retries: %v", task.writerJob.maxRetries+1, lastErr)
}

func (task *DorisWriterTask) getNextLoadUrl() string {
	url := task.writerJob.loadUrls[task.currentUrl]
	task.currentUrl = (task.currentUrl + 1) % len(task.writerJob.loadUrls)
	return url
}

func (task *DorisWriterTask) Post() error {
	return nil
}

func (task *DorisWriterTask) Destroy() error {
	if task.httpClient != nil {
		task.httpClient.CloseIdleConnections()
	}
	return nil
}