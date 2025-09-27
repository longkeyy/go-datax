package clickhousereader

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
	DefaultFetchSize = 1024
)

// ClickHouseReaderJob ClickHouse读取作业
type ClickHouseReaderJob struct {
	config    *config.Configuration
	username  string
	password  string
	jdbcUrls  []string
	tables    []string
	columns   []string
	where     string
	splitPk   string
	querySql  string
	fetchSize int
	session   map[string]string
}

func NewClickHouseReaderJob() *ClickHouseReaderJob {
	return &ClickHouseReaderJob{
		fetchSize: DefaultFetchSize,
		session:   make(map[string]string),
	}
}

func (job *ClickHouseReaderJob) Init(config *config.Configuration) error {
	job.config = config

	// 获取数据库连接参数
	job.username = config.GetString("parameter.username")
	job.password = config.GetString("parameter.password")

	if job.username == "" {
		return fmt.Errorf("username is required")
	}

	// 获取连接信息
	connections := config.GetListConfiguration("parameter.connection")
	if len(connections) == 0 {
		return fmt.Errorf("connection configuration is required")
	}

	// 处理第一个连接配置
	conn := connections[0]
	job.jdbcUrls = conn.GetStringList("jdbcUrl")

	if len(job.jdbcUrls) == 0 {
		return fmt.Errorf("jdbcUrl is required")
	}

	// 获取可选参数
	job.where = config.GetString("parameter.where")
	job.splitPk = config.GetString("parameter.splitPk")
	job.querySql = config.GetString("parameter.querySql")

	// 获取fetchSize
	if fetchSize := config.GetInt("parameter.fetchSize"); fetchSize > 0 {
		job.fetchSize = fetchSize
	}

	// 获取session配置
	if sessionConfig := config.GetMap("parameter.session"); sessionConfig != nil {
		for key, value := range sessionConfig {
			job.session[key] = fmt.Sprintf("%v", value)
		}
	}

	// 检查配置模式：querySql 或 table/column 模式
	if job.querySql != "" {
		// querySql模式：直接使用用户提供的SQL查询
		log.Printf("ClickHouse Reader initialized with querySql mode")
	} else {
		// table/column模式：需要table和column配置
		job.tables = conn.GetStringList("table")
		job.columns = config.GetStringList("parameter.column")

		if len(job.tables) == 0 {
			return fmt.Errorf("table is required when querySql is not specified")
		}
		if len(job.columns) == 0 {
			return fmt.Errorf("column is required when querySql is not specified")
		}
		log.Printf("ClickHouse Reader initialized with table/column mode: tables=%v, columns=%v", job.tables, job.columns)
	}

	return nil
}

func (job *ClickHouseReaderJob) Prepare() error {
	return nil
}

func (job *ClickHouseReaderJob) parseConnectionString(jdbcUrl string) (clickhouse.Options, error) {
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

func (job *ClickHouseReaderJob) getTableColumns(tableName string) ([]string, error) {
	if len(job.columns) == 1 && job.columns[0] == "*" {
		// 查询表的所有列
		options, err := job.parseConnectionString(job.jdbcUrls[0])
		if err != nil {
			return nil, err
		}

		conn, err := clickhouse.Open(&options)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to ClickHouse: %v", err)
		}
		defer conn.Close()

		query := `
			SELECT name
			FROM system.columns
			WHERE database = ? AND table = ?
			ORDER BY position`

		rows, err := conn.Query(context.Background(), query, options.Auth.Database, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get table columns: %v", err)
		}
		defer rows.Close()

		var columns []string
		for rows.Next() {
			var columnName string
			if err := rows.Scan(&columnName); err != nil {
				return nil, err
			}
			columns = append(columns, columnName)
		}

		if len(columns) == 0 {
			return nil, fmt.Errorf("table %s has no columns or does not exist", tableName)
		}

		return columns, nil
	}

	return job.columns, nil
}

func (job *ClickHouseReaderJob) Split(adviceNumber int) ([]*config.Configuration, error) {
	taskConfigs := make([]*config.Configuration, 0)

	// 如果有splitPk，按splitPk进行数据分片
	if job.splitPk != "" && adviceNumber > 1 {
		for _, table := range job.tables {
			tableConfigs, err := job.splitByPk(table, adviceNumber)
			if err != nil {
				return nil, err
			}
			taskConfigs = append(taskConfigs, tableConfigs...)
		}
	} else {
		// 按表进行分片
		for i, table := range job.tables {
			taskConfig := job.config.Clone()
			taskConfig.Set("taskId", i)
			taskConfig.Set("currentTable", table)
			taskConfigs = append(taskConfigs, taskConfig)
		}
	}

	log.Printf("Split into %d tasks", len(taskConfigs))
	return taskConfigs, nil
}

func (job *ClickHouseReaderJob) splitByPk(tableName string, adviceNumber int) ([]*config.Configuration, error) {
	// 连接数据库获取splitPk的范围
	options, err := job.parseConnectionString(job.jdbcUrls[0])
	if err != nil {
		return nil, err
	}

	conn, err := clickhouse.Open(&options)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %v", err)
	}
	defer conn.Close()

	// 获取splitPk的最小值和最大值
	query := fmt.Sprintf("SELECT min(%s) as min_val, max(%s) as max_val FROM %s",
		job.splitPk, job.splitPk, tableName)
	if job.where != "" {
		query += " WHERE " + job.where
	}

	row := conn.QueryRow(context.Background(), query)
	var minVal, maxVal int64
	err = row.Scan(&minVal, &maxVal)
	if err != nil {
		return nil, fmt.Errorf("failed to get splitPk range: %v", err)
	}

	// 计算分片范围
	taskConfigs := make([]*config.Configuration, 0)
	if maxVal <= minVal {
		// 只有一个分片
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", 0)
		taskConfig.Set("currentTable", tableName)
		taskConfigs = append(taskConfigs, taskConfig)
	} else {
		step := (maxVal - minVal) / int64(adviceNumber)
		if step <= 0 {
			step = 1
		}

		for i := 0; i < adviceNumber; i++ {
			taskConfig := job.config.Clone()
			taskConfig.Set("taskId", i)
			taskConfig.Set("currentTable", tableName)

			if i == 0 {
				taskConfig.Set("splitPkRange", map[string]interface{}{
					"min": minVal,
					"max": minVal + step,
				})
			} else if i == adviceNumber-1 {
				taskConfig.Set("splitPkRange", map[string]interface{}{
					"min": minVal + int64(i)*step + 1,
					"max": maxVal,
				})
			} else {
				taskConfig.Set("splitPkRange", map[string]interface{}{
					"min": minVal + int64(i)*step + 1,
					"max": minVal + int64(i+1)*step,
				})
			}

			taskConfigs = append(taskConfigs, taskConfig)
		}
	}

	return taskConfigs, nil
}

func (job *ClickHouseReaderJob) Post() error {
	return nil
}

func (job *ClickHouseReaderJob) Destroy() error {
	return nil
}

// ClickHouseReaderTask ClickHouse读取任务
type ClickHouseReaderTask struct {
	config    *config.Configuration
	readerJob *ClickHouseReaderJob
	conn      clickhouse.Conn
}

func NewClickHouseReaderTask() *ClickHouseReaderTask {
	return &ClickHouseReaderTask{}
}

func (task *ClickHouseReaderTask) Init(config *config.Configuration) error {
	task.config = config

	// 创建ReaderJob来重用配置逻辑
	task.readerJob = NewClickHouseReaderJob()
	err := task.readerJob.Init(config)
	if err != nil {
		return err
	}

	// 创建数据库连接
	options, err := task.readerJob.parseConnectionString(task.readerJob.jdbcUrls[0])
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

	log.Printf("ClickHouse connection established")
	return nil
}

func (task *ClickHouseReaderTask) Prepare() error {
	return nil
}

func (task *ClickHouseReaderTask) StartRead(recordSender plugin.RecordSender) error {
	tableName := task.config.GetString("currentTable")
	if tableName == "" && len(task.readerJob.tables) > 0 {
		tableName = task.readerJob.tables[0]
	}

	// 获取实际的列名
	columns, err := task.readerJob.getTableColumns(tableName)
	if err != nil {
		return err
	}

	// 构建查询SQL
	var query string
	if task.readerJob.querySql != "" {
		query = task.readerJob.querySql
	} else {
		query = task.buildQuery(tableName, columns)
	}

	log.Printf("Executing query: %s", query)

	// 执行查询
	rows, err := task.conn.Query(context.Background(), query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	// 获取列类型信息
	columnTypes := rows.ColumnTypes()

	recordCount := int64(0)
	for rows.Next() {
		// 创建扫描目标
		values := make([]interface{}, len(columnTypes))
		valuePtrs := make([]interface{}, len(columnTypes))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 扫描行数据
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		// 转换为DataX记录
		record := element.NewRecord()
		for i, value := range values {
			column := task.convertValue(value, columnTypes[i].DatabaseTypeName())
			record.AddColumn(column)
		}

		// 发送记录
		if err := recordSender.SendRecord(record); err != nil {
			return fmt.Errorf("failed to send record: %v", err)
		}

		recordCount++

		// 每1000条记录输出一次进度
		if recordCount%1000 == 0 {
			log.Printf("Read %d records", recordCount)
		}
	}

	log.Printf("Completed reading %d records", recordCount)
	return nil
}

func (task *ClickHouseReaderTask) buildQuery(tableName string, columns []string) string {
	query := "SELECT " + strings.Join(columns, ", ") + " FROM " + tableName

	// 添加WHERE条件
	conditions := make([]string, 0)

	if task.readerJob.where != "" {
		conditions = append(conditions, task.readerJob.where)
	}

	// 添加splitPk范围条件
	if splitPkRange := task.config.GetMap("splitPkRange"); splitPkRange != nil {
		if min, exists := splitPkRange["min"]; exists {
			if max, exists := splitPkRange["max"]; exists {
				condition := fmt.Sprintf("%s >= %v AND %s <= %v",
					task.readerJob.splitPk, min, task.readerJob.splitPk, max)
				conditions = append(conditions, condition)
			}
		}
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	return query
}

func (task *ClickHouseReaderTask) convertValue(value interface{}, typeName string) element.Column {
	if value == nil {
		return element.NewStringColumn("")
	}

	switch typeName {
	case "UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64":
		if intVal, err := strconv.ParseInt(fmt.Sprintf("%v", value), 10, 64); err == nil {
			return element.NewLongColumn(intVal)
		}
		return element.NewLongColumn(0)

	case "Float32", "Float64", "Decimal":
		if floatVal, err := strconv.ParseFloat(fmt.Sprintf("%v", value), 64); err == nil {
			return element.NewDoubleColumn(floatVal)
		}
		return element.NewDoubleColumn(0.0)

	case "Boolean":
		if boolVal, err := strconv.ParseBool(fmt.Sprintf("%v", value)); err == nil {
			return element.NewBoolColumn(boolVal)
		}
		return element.NewBoolColumn(false)

	case "Date", "Date32", "DateTime", "DateTime64":
		if timeVal, ok := value.(time.Time); ok {
			return element.NewDateColumn(timeVal)
		}
		return element.NewStringColumn(fmt.Sprintf("%v", value))

	case "Array(UInt8)":
		if bytesVal, ok := value.([]byte); ok {
			return element.NewBytesColumn(bytesVal)
		}
		return element.NewBytesColumn([]byte{})

	default: // String, FixedString, UUID, IPv4, IPv6, Array types
		if strings.HasPrefix(typeName, "Array(") {
			// Array类型转换为JSON字符串
			return element.NewStringColumn(fmt.Sprintf("%v", value))
		}
		return element.NewStringColumn(fmt.Sprintf("%v", value))
	}
}

func (task *ClickHouseReaderTask) Post() error {
	return nil
}

func (task *ClickHouseReaderTask) Destroy() error {
	if task.conn != nil {
		task.conn.Close()
	}
	return nil
}