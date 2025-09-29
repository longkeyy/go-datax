package clickhousewriter

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
)

const (
	DefaultBatchSize     = 65536
	DefaultBatchByteSize = 134217728 // 128MB
)

// ClickHouseWriterJob ClickHouse写入作业
type ClickHouseWriterJob struct {
	config        config.Configuration
	username      string
	password      string
	jdbcUrl       string
	tables        []string
	columns       []string
	batchSize     int
	batchByteSize int64
	writeMode     string
	preSql        []string
	postSql       []string
	session       map[string]string
}

func NewClickHouseWriterJob() *ClickHouseWriterJob {
	return &ClickHouseWriterJob{
		batchSize:     DefaultBatchSize,
		batchByteSize: DefaultBatchByteSize,
		writeMode:     "insert",
		session:       make(map[string]string),
	}
}

func (job *ClickHouseWriterJob) Init(config config.Configuration) error {
	job.config = config

	// 获取必需参数
	job.username = config.GetString("parameter.username")
	job.password = config.GetString("parameter.password")

	if job.username == "" {
		return fmt.Errorf("username is required")
	}

	// 获取连接配置
	connections := config.GetList("parameter.connection")
	if len(connections) == 0 {
		return fmt.Errorf("connection configuration is required")
	}

	if connMap, ok := connections[0].(map[string]interface{}); ok {
		job.jdbcUrl, _ = connMap["jdbcUrl"].(string)

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
	job.batchSize = config.GetIntWithDefault("parameter.batchSize", DefaultBatchSize)
	job.batchByteSize = int64(config.GetIntWithDefault("parameter.batchByteSize", DefaultBatchByteSize))
	job.writeMode = config.GetStringWithDefault("parameter.writeMode", "insert")

	job.preSql = config.GetStringList("parameter.preSql")
	job.postSql = config.GetStringList("parameter.postSql")

	// 获取session配置
	if sessionConfig := config.GetMap("parameter.session"); sessionConfig != nil {
		for key, value := range sessionConfig {
			job.session[key] = fmt.Sprintf("%v", value)
		}
	}

	log.Printf("ClickHouseWriter initialized: jdbcUrl=%s, table=%v, columns=%v",
		job.jdbcUrl, job.tables, job.columns)
	return nil
}

func (job *ClickHouseWriterJob) parseConnectionString(jdbcUrl string) (clickhouse.Options, error) {
	// 解析 clickhouse://host:port/database 格式的连接字符串
	if !strings.HasPrefix(jdbcUrl, "clickhouse://") {
		return clickhouse.Options{}, fmt.Errorf("invalid ClickHouse URL format: %s", jdbcUrl)
	}

	url := strings.TrimPrefix(jdbcUrl, "clickhouse://")
	parts := strings.Split(url, "/")
	if len(parts) != 2 {
		return clickhouse.Options{}, fmt.Errorf("invalid ClickHouse URL format: %s", jdbcUrl)
	}

	hostPort := parts[0]
	database := parts[1]

	var host string
	var port int = 9000 // 默认端口

	if strings.Contains(hostPort, ":") {
		hostPortParts := strings.Split(hostPort, ":")
		host = hostPortParts[0]
		if len(hostPortParts) > 1 {
			if p, err := strconv.Atoi(hostPortParts[1]); err == nil {
				port = p
			}
		}
	} else {
		host = hostPort
	}

	// 转换session配置为ClickHouse Settings
	settings := make(clickhouse.Settings)
	for k, v := range job.session {
		settings[k] = v
	}

	options := clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Database: database,
			Username: job.username,
			Password: job.password,
		},
		Settings: settings,
	}

	return options, nil
}

func (job *ClickHouseWriterJob) Prepare() error {
	// 执行pre SQL语句
	if len(job.preSql) > 0 {
		err := job.executeSql(job.preSql)
		if err != nil {
			return fmt.Errorf("failed to execute preSql: %v", err)
		}
	}
	return nil
}

func (job *ClickHouseWriterJob) executeSql(sqlList []string) error {
	options, err := job.parseConnectionString(job.jdbcUrl)
	if err != nil {
		return err
	}

	conn, err := clickhouse.Open(&options)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %v", err)
	}
	defer conn.Close()

	for _, sql := range sqlList {
		log.Printf("Executing SQL: %s", sql)
		err := conn.Exec(context.Background(), sql)
		if err != nil {
			return fmt.Errorf("failed to execute SQL '%s': %v", sql, err)
		}
	}

	return nil
}

func (job *ClickHouseWriterJob) Split(adviceNumber int) ([]config.Configuration, error) {
	taskConfigs := make([]config.Configuration, 0)

	// 创建指定数量的任务配置
	for i := 0; i < adviceNumber; i++ {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	log.Printf("Split into %d tasks", len(taskConfigs))
	return taskConfigs, nil
}

func (job *ClickHouseWriterJob) Post() error {
	// 执行post SQL语句
	if len(job.postSql) > 0 {
		err := job.executeSql(job.postSql)
		if err != nil {
			return fmt.Errorf("failed to execute postSql: %v", err)
		}
	}
	return nil
}

func (job *ClickHouseWriterJob) Destroy() error {
	return nil
}

// ClickHouseWriterTask ClickHouse写入任务
type ClickHouseWriterTask struct {
	config      config.Configuration
	writerJob   *ClickHouseWriterJob
	conn        clickhouse.Conn
	buffer      [][]interface{}
	bufferSize  int64
	insertStmt  string
}

func NewClickHouseWriterTask() *ClickHouseWriterTask {
	return &ClickHouseWriterTask{
		buffer: make([][]interface{}, 0),
	}
}

func (task *ClickHouseWriterTask) Init(config config.Configuration) error {
	task.config = config

	// 创建WriterJob来重用配置逻辑
	task.writerJob = NewClickHouseWriterJob()
	err := task.writerJob.Init(config)
	if err != nil {
		return err
	}

	// 创建数据库连接
	options, err := task.writerJob.parseConnectionString(task.writerJob.jdbcUrl)
	if err != nil {
		return err
	}

	task.conn, err = clickhouse.Open(&options)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %v", err)
	}

	// 测试连接
	if err := task.conn.Ping(context.Background()); err != nil {
		return fmt.Errorf("failed to ping ClickHouse: %v", err)
	}

	// 构建插入语句
	tableName := task.writerJob.tables[0]
	placeholders := make([]string, len(task.writerJob.columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	task.insertStmt = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(task.writerJob.columns, ", "),
		strings.Join(placeholders, ", "))

	log.Printf("ClickHouseWriter task initialized: %s", task.insertStmt)
	return nil
}

func (task *ClickHouseWriterTask) Prepare() error {
	return nil
}

func (task *ClickHouseWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	defer func() {
		// 写入剩余的记录
		if len(task.buffer) > 0 {
			task.flushBuffer()
		}
		// 关闭连接
		if task.conn != nil {
			task.conn.Close()
		}
	}()

	recordCount := int64(0)

	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err.Error() == "channel is closed" {
				break
			}
			return fmt.Errorf("failed to receive record: %v", err)
		}

		// 转换记录为ClickHouse行数据
		row := task.recordToRow(record)
		task.buffer = append(task.buffer, row)
		task.bufferSize += task.estimateRowSize(row)

		// 检查是否需要刷新缓冲区
		shouldFlush := false

		// 条件1：达到最大行数
		if len(task.buffer) >= task.writerJob.batchSize {
			shouldFlush = true
		}

		// 条件2：达到最大字节数
		if task.bufferSize >= task.writerJob.batchByteSize {
			shouldFlush = true
		}

		if shouldFlush {
			if err := task.flushBuffer(); err != nil {
				return err
			}
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

func (task *ClickHouseWriterTask) recordToRow(record element.Record) []interface{} {
	columnCount := record.GetColumnNumber()
	row := make([]interface{}, len(task.writerJob.columns))

	for i := 0; i < len(task.writerJob.columns) && i < columnCount; i++ {
		column := record.GetColumn(i)
		if column == nil {
			row[i] = nil
			continue
		}

		row[i] = task.convertColumnValue(column)
	}

	return row
}

func (task *ClickHouseWriterTask) convertColumnValue(column element.Column) interface{} {
	if column.IsNull() {
		return nil
	}

	switch column.GetType() {
	case element.TypeLong:
		if val, err := column.GetAsLong(); err == nil {
			return val
		}
		return int64(0)

	case element.TypeDouble:
		if val, err := column.GetAsDouble(); err == nil {
			return val
		}
		return float64(0)

	case element.TypeBool:
		if val, err := column.GetAsBool(); err == nil {
			return val
		}
		return false

	case element.TypeDate:
		if val, err := column.GetAsDate(); err == nil {
			return val
		}
		return time.Now()

	case element.TypeBytes:
		if val, err := column.GetAsBytes(); err == nil {
			return val
		}
		return []byte{}

	default: // string
		return column.GetAsString()
	}
}

func (task *ClickHouseWriterTask) estimateRowSize(row []interface{}) int64 {
	size := int64(0)
	for _, value := range row {
		if value != nil {
			size += int64(len(fmt.Sprintf("%v", value)))
		}
	}
	return size + int64(len(row)*8) // 估算每个字段8字节开销
}

func (task *ClickHouseWriterTask) flushBuffer() error {
	if len(task.buffer) == 0 {
		return nil
	}

	batch, err := task.conn.PrepareBatch(context.Background(), task.insertStmt)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %v", err)
	}

	for _, row := range task.buffer {
		err := batch.Append(row...)
		if err != nil {
			return fmt.Errorf("failed to append row to batch: %v", err)
		}
	}

	err = batch.Send()
	if err != nil {
		return fmt.Errorf("failed to send batch: %v", err)
	}

	log.Printf("Successfully inserted %d records to ClickHouse", len(task.buffer))

	// 清空缓冲区
	task.buffer = task.buffer[:0]
	task.bufferSize = 0

	return nil
}

func (task *ClickHouseWriterTask) Post() error {
	return nil
}

func (task *ClickHouseWriterTask) Destroy() error {
	if task.conn != nil {
		task.conn.Close()
	}
	return nil
}