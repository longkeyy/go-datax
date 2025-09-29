package tdenginereader

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

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
	DateTimeFormat = "2006-01-02 15:04:05"
	DefaultWhere   = "_c0 > -9223372036854775808" // Long.MIN_VALUE equivalent
)

// TDengineReaderJob TDengine读取作业
type TDengineReaderJob struct {
	config        config.Configuration
	username      string
	password      string
	connections   []ConnectionConfig
	columns       []string
	beginDateTime string
	endDateTime   string
	where         string
	factory       *factory.DataXFactory
}

// ConnectionConfig 连接配置
type ConnectionConfig struct {
	JdbcUrls []string
	Tables   []string
	QuerySql []string
}

func NewTDengineReaderJob() *TDengineReaderJob {
	return &TDengineReaderJob{
		factory: factory.GetGlobalFactory(),
	}
}

func (job *TDengineReaderJob) Init(config config.Configuration) error {
	job.config = config

	log := logger.Component().WithComponent("TDengineReaderJob")

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

	for i, connItem := range connectionList {
		connConfig := ConnectionConfig{}

		// 解析jdbcUrl
		jdbcUrls := make([]string, 0)
		if jdbcUrlList, ok := connItem.(map[string]interface{})["jdbcUrl"]; ok {
			switch v := jdbcUrlList.(type) {
			case []interface{}:
				for _, url := range v {
					if urlStr, ok := url.(string); ok {
						jdbcUrls = append(jdbcUrls, urlStr)
					}
				}
			case string:
				jdbcUrls = append(jdbcUrls, v)
			}
		}
		if len(jdbcUrls) == 0 {
			return fmt.Errorf("parameter [jdbcUrl] of connection[%d] is not set", i+1)
		}
		connConfig.JdbcUrls = jdbcUrls

		// 解析table或querySql
		if querySqlList, ok := connItem.(map[string]interface{})["querySql"]; ok {
			switch v := querySqlList.(type) {
			case []interface{}:
				for _, sql := range v {
					if sqlStr, ok := sql.(string); ok {
						connConfig.QuerySql = append(connConfig.QuerySql, sqlStr)
					}
				}
			case string:
				if v != "" {
					connConfig.QuerySql = append(connConfig.QuerySql, v)
				}
			}
		}

		if len(connConfig.QuerySql) == 0 {
			// 如果没有querySql，检查table
			if tableList, ok := connItem.(map[string]interface{})["table"]; ok {
				switch v := tableList.(type) {
				case []interface{}:
					for _, table := range v {
						if tableStr, ok := table.(string); ok {
							connConfig.Tables = append(connConfig.Tables, tableStr)
						}
					}
				case string:
					connConfig.Tables = append(connConfig.Tables, v)
				}
			}
			if len(connConfig.Tables) == 0 {
				return fmt.Errorf("parameter [table] of connection[%d] is not set", i+1)
			}
		}

		job.connections = append(job.connections, connConfig)
	}

	// 获取列配置
	if columnList := config.GetList("parameter.column"); len(columnList) > 0 {
		for _, col := range columnList {
			if colStr, ok := col.(string); ok {
				job.columns = append(job.columns, colStr)
			}
		}
	}
	if len(job.columns) == 0 {
		job.columns = []string{"*"}
	}

	// 解析时间范围
	job.beginDateTime = config.GetString("parameter.beginDateTime")
	job.endDateTime = config.GetString("parameter.endDateTime")

	// 验证时间格式
	if job.beginDateTime != "" {
		if _, err := time.Parse(DateTimeFormat, job.beginDateTime); err != nil {
			return fmt.Errorf("parameter [beginDateTime] needs to conform to the [%s] format", DateTimeFormat)
		}
	}
	if job.endDateTime != "" {
		if _, err := time.Parse(DateTimeFormat, job.endDateTime); err != nil {
			return fmt.Errorf("parameter [endDateTime] needs to conform to the [%s] format", DateTimeFormat)
		}
	}

	// 检查时间范围的合理性
	if job.beginDateTime != "" && job.endDateTime != "" {
		beginTime, _ := time.Parse(DateTimeFormat, job.beginDateTime)
		endTime, _ := time.Parse(DateTimeFormat, job.endDateTime)
		if !beginTime.Before(endTime) {
			return fmt.Errorf("parameter [beginDateTime] should be less than parameter [endDateTime]")
		}
	}

	// 获取where条件
	job.where = config.GetString("parameter.where")
	if job.where == "" {
		job.where = DefaultWhere
	}

	log.Info("TDengine reader job initialized",
		zap.String("username", job.username),
		zap.Int("connections", len(job.connections)),
		zap.Strings("columns", job.columns),
		zap.String("beginDateTime", job.beginDateTime),
		zap.String("endDateTime", job.endDateTime),
		zap.String("where", job.where))

	return nil
}

func (job *TDengineReaderJob) Destroy() error {
	return nil
}

func (job *TDengineReaderJob) Post() error {
	return nil
}

func (job *TDengineReaderJob) Split(adviceNumber int) ([]config.Configuration, error) {
	log := logger.Component().WithComponent("TDengineReaderJob")

	configurations := make([]config.Configuration, 0)

	for _, conn := range job.connections {
		for _, jdbcUrl := range conn.JdbcUrls {
			// 为每个jdbcUrl创建一个任务配置
			taskConfig := config.NewConfiguration()

			// 复制基本配置
			taskConfig.Set("parameter.username", job.username)
			taskConfig.Set("parameter.password", job.password)
			taskConfig.Set("parameter.jdbcUrl", jdbcUrl)
			taskConfig.Set("parameter.column", job.columns)
			taskConfig.Set("parameter.beginDateTime", job.beginDateTime)
			taskConfig.Set("parameter.endDateTime", job.endDateTime)
			taskConfig.Set("parameter.where", job.where)

			// 设置表或查询SQL
			if len(conn.QuerySql) > 0 {
				taskConfig.Set("parameter.querySql", conn.QuerySql)
			} else {
				taskConfig.Set("parameter.table", conn.Tables)
			}

			// 设置编码（如果有的话）
			if encoding := job.config.GetString("parameter.mandatoryEncoding"); encoding != "" {
				taskConfig.Set("parameter.mandatoryEncoding", encoding)
			}

			configurations = append(configurations, taskConfig)
		}
	}

	log.Info("Split into tasks", zap.Int("taskCount", len(configurations)))
	return configurations, nil
}

// TDengineReaderTask TDengine读取任务
type TDengineReaderTask struct {
	config            config.Configuration
	jdbcUrl           string
	username          string
	password          string
	tables            []string
	columns           []string
	beginDateTime     string
	endDateTime       string
	where             string
	querySql          []string
	mandatoryEncoding string
	db                *sql.DB
	factory           *factory.DataXFactory
}

func NewTDengineReaderTask() *TDengineReaderTask {
	return &TDengineReaderTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *TDengineReaderTask) Init(config config.Configuration) error {
	task.config = config

	log := logger.Component().WithComponent("TDengineReaderTask")

	// 获取配置参数
	task.username = config.GetString("parameter.username")
	task.password = config.GetString("parameter.password")
	task.jdbcUrl = config.GetString("parameter.jdbcUrl")
	task.beginDateTime = config.GetString("parameter.beginDateTime")
	task.endDateTime = config.GetString("parameter.endDateTime")
	task.where = config.GetString("parameter.where")
	task.mandatoryEncoding = config.GetString("parameter.mandatoryEncoding")
	if task.mandatoryEncoding == "" {
		task.mandatoryEncoding = "UTF-8"
	}

	// 获取列配置
	if columnList := config.GetList("parameter.column"); len(columnList) > 0 {
		for _, col := range columnList {
			if colStr, ok := col.(string); ok {
				task.columns = append(task.columns, colStr)
			}
		}
	}

	// 获取表配置
	if tableList := config.GetList("parameter.table"); len(tableList) > 0 {
		for _, table := range tableList {
			if tableStr, ok := table.(string); ok {
				task.tables = append(task.tables, tableStr)
			}
		}
	}

	// 获取查询SQL配置
	if querySqlList := config.GetList("parameter.querySql"); len(querySqlList) > 0 {
		for _, sql := range querySqlList {
			if sqlStr, ok := sql.(string); ok {
				task.querySql = append(task.querySql, sqlStr)
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

	log.Info("TDengine reader task initialized",
		zap.String("jdbcUrl", task.jdbcUrl),
		zap.String("username", task.username),
		zap.Strings("tables", task.tables),
		zap.Strings("columns", task.columns),
		zap.Int("querySqlCount", len(task.querySql)))

	return nil
}

func (task *TDengineReaderTask) Destroy() error {
	if task.db != nil {
		return task.db.Close()
	}
	return nil
}

func (task *TDengineReaderTask) Post() error {
	return nil
}

func (task *TDengineReaderTask) StartRead(recordSender plugin.RecordSender) error {
	log := logger.Component().WithComponent("TDengineReaderTask")

	sqlList := make([]string, 0)

	// 如果有自定义查询SQL，直接使用
	if len(task.querySql) > 0 {
		sqlList = append(sqlList, task.querySql...)
	} else {
		// 否则根据表和条件构建查询SQL
		for _, table := range task.tables {
			query := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
				strings.Join(task.columns, ","), table, task.where)

			// 添加时间范围条件
			if task.beginDateTime != "" {
				query += fmt.Sprintf(" AND _c0 >= '%s'", task.beginDateTime)
			}
			if task.endDateTime != "" {
				query += fmt.Sprintf(" AND _c0 < '%s'", task.endDateTime)
			}

			sqlList = append(sqlList, query)
		}
	}

	// 执行查询
	recordCount := int64(0)
	for _, query := range sqlList {
		log.Info("Executing query", zap.String("sql", query))

		rows, err := task.db.Query(query)
		if err != nil {
			log.Error("Failed to execute query", zap.String("sql", query), zap.Error(err))
			continue
		}

		// 获取列信息
		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			rows.Close()
			log.Error("Failed to get column types", zap.Error(err))
			continue
		}

		// 处理查询结果
		for rows.Next() {
			record, err := task.buildRecord(recordSender, rows, columnTypes)
			if err != nil {
				log.Error("Failed to build record", zap.Error(err))
				continue
			}

			recordSender.SendRecord(record)
			recordCount++
		}

		rows.Close()
	}

	log.Info("TDengine read completed", zap.Int64("recordCount", recordCount))
	return nil
}

func (task *TDengineReaderTask) buildRecord(recordSender plugin.RecordSender, rows *sql.Rows, columnTypes []*sql.ColumnType) (element.Record, error) {
	record := task.factory.GetRecordFactory().CreateRecord()

	// 创建扫描目标
	values := make([]interface{}, len(columnTypes))
	scanArgs := make([]interface{}, len(columnTypes))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// 扫描行数据
	if err := rows.Scan(scanArgs...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	// 根据数据类型转换并添加到记录
	for i, columnType := range columnTypes {
		var column element.Column

		if values[i] == nil {
			column = element.NewStringColumn("")
		} else {
			switch columnType.DatabaseTypeName() {
			case "TINYINT", "SMALLINT", "INT", "BIGINT":
				if val, err := convertToInt64(values[i]); err == nil {
					column = element.NewLongColumn(val)
				} else {
					column = element.NewStringColumn(fmt.Sprintf("%v", values[i]))
				}
			case "FLOAT", "DOUBLE":
				if val, err := convertToFloat64(values[i]); err == nil {
					column = element.NewDoubleColumn(val)
				} else {
					column = element.NewStringColumn(fmt.Sprintf("%v", values[i]))
				}
			case "BOOL":
				if val, ok := values[i].(bool); ok {
					column = element.NewBoolColumn(val)
				} else {
					column = element.NewStringColumn(fmt.Sprintf("%v", values[i]))
				}
			case "TIMESTAMP":
				if val, ok := values[i].(time.Time); ok {
					column = element.NewDateColumn(val)
				} else {
					column = element.NewStringColumn(fmt.Sprintf("%v", values[i]))
				}
			case "BINARY":
				if val, ok := values[i].([]byte); ok {
					column = element.NewBytesColumn(val)
				} else {
					column = element.NewStringColumn(fmt.Sprintf("%v", values[i]))
				}
			default: // NCHAR and others
				column = element.NewStringColumn(fmt.Sprintf("%v", values[i]))
			}
		}

		record.AddColumn(column)
	}

	return record, nil
}

// convertToInt64 将值转换为int64
func convertToInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", value)
	}
}

// convertToFloat64 将值转换为float64
func convertToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(v, 64)
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}