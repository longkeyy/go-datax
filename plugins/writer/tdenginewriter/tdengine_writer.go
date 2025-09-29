package tdenginewriter

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/factory"
	"go.uber.org/zap"

	// TDengine driver (使用restful协议避免CGO依赖)
	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

const (
	DefaultBatchSize           = 1000
	DefaultIgnoreTagsUnmatched = false
)

// TDengineWriterJob TDengine写入作业
type TDengineWriterJob struct {
	config              config.Configuration
	username            string
	password            string
	jdbcUrl             string
	tables              []string
	columns             []string
	batchSize           int
	ignoreTagsUnmatched bool
	factory             *factory.DataXFactory
}

func NewTDengineWriterJob() *TDengineWriterJob {
	return &TDengineWriterJob{
		factory: factory.GetGlobalFactory(),
	}
}

func (job *TDengineWriterJob) Init(config config.Configuration) error {
	job.config = config

	log := logger.Component().WithComponent("TDengineWriterJob")

	// 检查必需参数
	job.username = config.GetString("parameter.username")
	if job.username == "" {
		return fmt.Errorf("parameter [username] is not set")
	}

	job.password = config.GetString("parameter.password")
	if job.password == "" {
		return fmt.Errorf("parameter [password] is not set")
	}

	// 解析连接配置
	connectionList := config.GetList("parameter.connection")
	if len(connectionList) == 0 {
		return fmt.Errorf("parameter [connection] is not set")
	}

	if len(connectionList) > 1 {
		log.Warn("connection.size is greater than 1 and only connection[0] will be used")
	}

	// 获取第一个连接的配置
	if connItem, ok := connectionList[0].(map[string]interface{}); ok {
		if jdbcUrl, ok := connItem["jdbcUrl"].(string); ok {
			job.jdbcUrl = jdbcUrl
		} else {
			return fmt.Errorf("parameter [jdbcUrl] of connection is not set")
		}

		// 获取表列表
		if tableList, ok := connItem["table"]; ok {
			switch v := tableList.(type) {
			case []interface{}:
				for _, table := range v {
					if tableStr, ok := table.(string); ok {
						job.tables = append(job.tables, tableStr)
					}
				}
			case string:
				job.tables = append(job.tables, v)
			}
		}
	} else {
		return fmt.Errorf("invalid connection configuration")
	}

	// 获取列配置
	if columnList := config.GetList("parameter.column"); len(columnList) > 0 {
		for _, col := range columnList {
			if colStr, ok := col.(string); ok {
				job.columns = append(job.columns, colStr)
			}
		}
	}

	// 获取批处理大小
	job.batchSize = config.GetInt("parameter.batchSize")
	if job.batchSize <= 0 {
		job.batchSize = DefaultBatchSize
	}

	// 获取是否忽略标签不匹配
	job.ignoreTagsUnmatched = config.GetBool("parameter.ignoreTagsUnmatched")

	log.Info("TDengine writer job initialized",
		zap.String("username", job.username),
		zap.String("jdbcUrl", job.jdbcUrl),
		zap.Strings("tables", job.tables),
		zap.Strings("columns", job.columns),
		zap.Int("batchSize", job.batchSize),
		zap.Bool("ignoreTagsUnmatched", job.ignoreTagsUnmatched))

	return nil
}

func (job *TDengineWriterJob) Destroy() error {
	return nil
}

func (job *TDengineWriterJob) Post() error {
	return nil
}

func (job *TDengineWriterJob) Prepare() error {
	return nil
}

func (job *TDengineWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	configurations := make([]config.Configuration, 0)

	for i := 0; i < mandatoryNumber; i++ {
		taskConfig := config.NewConfiguration()

		// 复制配置到任务配置
		taskConfig.Set("parameter.username", job.username)
		taskConfig.Set("parameter.password", job.password)
		taskConfig.Set("parameter.jdbcUrl", job.jdbcUrl)
		taskConfig.Set("parameter.table", job.tables)
		taskConfig.Set("parameter.column", job.columns)
		taskConfig.Set("parameter.batchSize", job.batchSize)
		taskConfig.Set("parameter.ignoreTagsUnmatched", job.ignoreTagsUnmatched)

		configurations = append(configurations, taskConfig)
	}

	return configurations, nil
}

// TDengineWriterTask TDengine写入任务
type TDengineWriterTask struct {
	config              config.Configuration
	username            string
	password            string
	jdbcUrl             string
	tables              []string
	columns             []string
	batchSize           int
	ignoreTagsUnmatched bool
	db                  *sql.DB
	factory             *factory.DataXFactory
}

func NewTDengineWriterTask() *TDengineWriterTask {
	return &TDengineWriterTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *TDengineWriterTask) Init(config config.Configuration) error {
	task.config = config

	log := logger.Component().WithComponent("TDengineWriterTask")

	// 获取配置参数
	task.username = config.GetString("parameter.username")
	task.password = config.GetString("parameter.password")
	task.jdbcUrl = config.GetString("parameter.jdbcUrl")
	task.batchSize = config.GetInt("parameter.batchSize")
	task.ignoreTagsUnmatched = config.GetBool("parameter.ignoreTagsUnmatched")

	// 获取表配置
	if tableList := config.GetList("parameter.table"); len(tableList) > 0 {
		for _, table := range tableList {
			if tableStr, ok := table.(string); ok {
				task.tables = append(task.tables, tableStr)
			}
		}
	}

	// 获取列配置
	if columnList := config.GetList("parameter.column"); len(columnList) > 0 {
		for _, col := range columnList {
			if colStr, ok := col.(string); ok {
				task.columns = append(task.columns, colStr)
			}
		}
	}

	// 建立数据库连接
	db, err := sql.Open("taosRestful", fmt.Sprintf("%s?user=%s&password=%s", task.jdbcUrl, task.username, task.password))
	if err != nil {
		return fmt.Errorf("failed to connect to TDengine: %w", err)
	}

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping TDengine: %w", err)
	}

	task.db = db

	log.Info("TDengine writer task initialized",
		zap.String("jdbcUrl", task.jdbcUrl),
		zap.String("username", task.username),
		zap.Strings("tables", task.tables),
		zap.Strings("columns", task.columns),
		zap.Int("batchSize", task.batchSize))

	return nil
}

func (task *TDengineWriterTask) Destroy() error {
	if task.db != nil {
		return task.db.Close()
	}
	return nil
}

func (task *TDengineWriterTask) Post() error {
	return nil
}

func (task *TDengineWriterTask) Prepare() error {
	return nil
}

func (task *TDengineWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	log := logger.Component().WithComponent("TDengineWriterTask")

	recordBatch := make([]element.Record, 0, task.batchSize)
	totalRecords := int64(0)
	affectedRows := int64(0)

	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			log.Error("Failed to get record from reader", zap.Error(err))
			break
		}
		if record == nil {
			break
		}

		recordBatch = append(recordBatch, record)
		totalRecords++

		// 当批次达到指定大小或没有更多记录时，写入批次
		if len(recordBatch) >= task.batchSize {
			if rows, err := task.writeBatch(recordBatch); err != nil {
				log.Error("Failed to write batch", zap.Error(err))
				// 尝试逐条写入
				if rows, err := task.writeEachRow(recordBatch); err != nil {
					log.Error("Failed to write records individually", zap.Error(err))
				} else {
					affectedRows += int64(rows)
				}
			} else {
				affectedRows += int64(rows)
			}
			recordBatch = recordBatch[:0] // 清空批次
		}
	}

	// 写入剩余的记录
	if len(recordBatch) > 0 {
		if rows, err := task.writeBatch(recordBatch); err != nil {
			log.Error("Failed to write final batch", zap.Error(err))
			// 尝试逐条写入
			if rows, err := task.writeEachRow(recordBatch); err != nil {
				log.Error("Failed to write final records individually", zap.Error(err))
			} else {
				affectedRows += int64(rows)
			}
		} else {
			affectedRows += int64(rows)
		}
	}

	log.Info("TDengine write completed",
		zap.Int64("totalRecords", totalRecords),
		zap.Int64("affectedRows", affectedRows))

	return nil
}

// writeBatch 批量写入记录
func (task *TDengineWriterTask) writeBatch(records []element.Record) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	log := logger.Component().WithComponent("TDengineWriterTask")
	affectedRows := 0

	// 对每个表执行批量插入
	for _, table := range task.tables {
		sql, err := task.buildInsertSQL(table, records)
		if err != nil {
			return 0, fmt.Errorf("failed to build insert SQL: %w", err)
		}

		log.Debug("Executing batch insert", zap.String("sql", sql[:min(len(sql), 200)]+"..."))

		result, err := task.db.Exec(sql)
		if err != nil {
			return 0, fmt.Errorf("failed to execute batch insert: %w", err)
		}

		if rowsAffected, err := result.RowsAffected(); err == nil {
			affectedRows += int(rowsAffected)
		}
	}

	return affectedRows, nil
}

// writeEachRow 逐条写入记录
func (task *TDengineWriterTask) writeEachRow(records []element.Record) (int, error) {
	affectedRows := 0

	for _, record := range records {
		if rows, err := task.writeBatch([]element.Record{record}); err != nil {
			// 记录脏数据，但继续处理其他记录
			logger.Component().WithComponent("TDengineWriterTask").
				Error("Failed to write single record", zap.Error(err))
		} else {
			affectedRows += rows
		}
	}

	return affectedRows, nil
}

// buildInsertSQL 构建插入SQL语句
func (task *TDengineWriterTask) buildInsertSQL(table string, records []element.Record) (string, error) {
	if len(records) == 0 {
		return "", fmt.Errorf("no records to insert")
	}

	var sql strings.Builder

	// 构建INSERT INTO语句的开头
	if len(task.columns) > 0 {
		sql.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
			table, strings.Join(task.columns, ", ")))
	} else {
		sql.WriteString(fmt.Sprintf("INSERT INTO %s VALUES ", table))
	}

	// 构建VALUES部分
	for i, record := range records {
		if i > 0 {
			sql.WriteString(", ")
		}

		sql.WriteString("(")
		columnCount := record.GetColumnNumber()

		for j := 0; j < columnCount; j++ {
			if j > 0 {
				sql.WriteString(", ")
			}

			column := record.GetColumn(j)
			value, err := task.convertColumnValue(column)
			if err != nil {
				return "", fmt.Errorf("failed to convert column %d: %w", j, err)
			}
			sql.WriteString(value)
		}

		sql.WriteString(")")
	}

	return sql.String(), nil
}

// convertColumnValue 转换列值为SQL字符串
func (task *TDengineWriterTask) convertColumnValue(column element.Column) (string, error) {
	if column == nil {
		return "NULL", nil
	}

	switch column.GetType() {
	case element.TypeBool:
		if val, err := column.GetAsBool(); err == nil {
			if val {
				return "true", nil
			}
			return "false", nil
		}
		return "NULL", nil

	case element.TypeLong:
		if val, err := column.GetAsLong(); err == nil {
			return strconv.FormatInt(val, 10), nil
		}
		return "NULL", nil

	case element.TypeDouble:
		if val, err := column.GetAsDouble(); err == nil {
			return strconv.FormatFloat(val, 'f', -1, 64), nil
		}
		return "NULL", nil

	case element.TypeString:
		val := column.GetAsString()
		// 转义单引号
		escaped := strings.ReplaceAll(val, "'", "''")
		return fmt.Sprintf("'%s'", escaped), nil

	case element.TypeDate:
		if val, err := column.GetAsDate(); err == nil {
			return fmt.Sprintf("'%s'", val.Format("2006-01-02 15:04:05.000")), nil
		}
		return "NULL", nil

	case element.TypeBytes:
		if val, err := column.GetAsBytes(); err == nil {
			// TDengine二进制数据需要特殊处理
			return fmt.Sprintf("'%s'", string(val)), nil
		}
		return "NULL", nil

	default:
		// 对于未知类型，尝试转换为字符串
		val := column.GetAsString()
		escaped := strings.ReplaceAll(val, "'", "''")
		return fmt.Sprintf("'%s'", escaped), nil
	}
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}