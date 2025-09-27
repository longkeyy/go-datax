package mysqlreader

import (
	"database/sql"
	"fmt"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"log"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	DefaultFetchSize = 1024
)

// MySQLReaderJob MySQL读取作业
type MySQLReaderJob struct {
	config   *config.Configuration
	username string
	password string
	jdbcUrls []string
	tables   []string
	columns  []string
	where    string
	splitPk  string
}

func NewMySQLReaderJob() *MySQLReaderJob {
	return &MySQLReaderJob{}
}

func (job *MySQLReaderJob) Init(config *config.Configuration) error {
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

	// 获取列信息
	job.columns = config.GetStringList("parameter.column")
	if len(job.columns) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	// 获取可选参数
	job.where = config.GetString("parameter.where")
	job.splitPk = config.GetString("parameter.splitPk")

	log.Printf("MySQL Reader initialized: tables=%v, columns=%v", job.tables, job.columns)
	return nil
}

func (job *MySQLReaderJob) Split(adviceNumber int) ([]*config.Configuration, error) {
	taskConfigs := make([]*config.Configuration, 0)

	// 如果没有设置splitPk，则不进行分片，使用单个任务
	if job.splitPk == "" {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", 0)
		taskConfigs = append(taskConfigs, taskConfig)
		log.Printf("No splitPk specified, using single task")
		return taskConfigs, nil
	}

	// 如果设置了splitPk，尝试进行分片
	ranges, err := job.calculateSplitRanges(adviceNumber)
	if err != nil {
		log.Printf("Failed to calculate split ranges, fallback to single task: %v", err)
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", 0)
		taskConfigs = append(taskConfigs, taskConfig)
		return taskConfigs, nil
	}

	// 创建分片任务配置
	for i, splitRange := range ranges {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfig.Set("parameter.splitRange", splitRange)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	log.Printf("Split into %d tasks based on splitPk: %s", len(taskConfigs), job.splitPk)
	return taskConfigs, nil
}

func (job *MySQLReaderJob) calculateSplitRanges(adviceNumber int) ([]map[string]interface{}, error) {
	// 连接数据库获取splitPk的最小值和最大值
	db, err := job.connect()
	if err != nil {
		return nil, err
	}
	defer func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	table := job.tables[0]
	query := fmt.Sprintf("SELECT MIN(`%s`), MAX(`%s`) FROM `%s`", job.splitPk, job.splitPk, table)
	if job.where != "" {
		query += fmt.Sprintf(" WHERE %s", job.where)
	}

	var minVal, maxVal sql.NullInt64
	err = db.Raw(query).Row().Scan(&minVal, &maxVal)
	if err != nil {
		return nil, err
	}

	if !minVal.Valid || !maxVal.Valid {
		return nil, fmt.Errorf("splitPk column contains null values")
	}

	ranges := make([]map[string]interface{}, 0)
	step := (maxVal.Int64 - minVal.Int64 + int64(adviceNumber) - 1) / int64(adviceNumber)

	if step <= 0 {
		step = 1
	}

	for i := 0; i < adviceNumber; i++ {
		rangeStart := minVal.Int64 + int64(i)*step
		rangeEnd := minVal.Int64 + int64(i+1)*step - 1

		if i == adviceNumber-1 {
			rangeEnd = maxVal.Int64
		}

		ranges = append(ranges, map[string]interface{}{
			"start": rangeStart,
			"end":   rangeEnd,
		})
	}

	return ranges, nil
}

func (job *MySQLReaderJob) connect() (*gorm.DB, error) {
	// 构建连接字符串
	jdbcUrl := job.jdbcUrls[0]

	// 转换JDBC URL为MySQL连接字符串
	dsn, err := job.convertJdbcUrl(jdbcUrl)
	if err != nil {
		return nil, err
	}

	// 连接数据库
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	return db, nil
}

func (job *MySQLReaderJob) convertJdbcUrl(jdbcUrl string) (string, error) {
	// 解析JDBC URL: jdbc:mysql://host:port/database?参数
	if !strings.HasPrefix(jdbcUrl, "jdbc:mysql://") {
		return "", fmt.Errorf("invalid MySQL JDBC URL: %s", jdbcUrl)
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

	// 构建MySQL DSN字符串
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&loc=Local",
		job.username,
		job.password,
		hostPort,
		database)

	// 添加额外参数
	if params != "" {
		dsn += "&" + params
	}

	return dsn, nil
}

func (job *MySQLReaderJob) Post() error {
	return nil
}

func (job *MySQLReaderJob) Destroy() error {
	return nil
}

// MySQLReaderTask MySQL读取任务
type MySQLReaderTask struct {
	config    *config.Configuration
	readerJob *MySQLReaderJob
	db        *gorm.DB
}

func NewMySQLReaderTask() *MySQLReaderTask {
	return &MySQLReaderTask{}
}

func (task *MySQLReaderTask) Init(config *config.Configuration) error {
	task.config = config

	// 创建ReaderJob来重用连接逻辑
	task.readerJob = NewMySQLReaderJob()
	err := task.readerJob.Init(config)
	if err != nil {
		return err
	}

	// 建立数据库连接
	task.db, err = task.readerJob.connect()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	return nil
}

func (task *MySQLReaderTask) StartRead(recordSender plugin.RecordSender) error {
	defer func() {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	// 构建查询SQL
	query, err := task.buildQuery()
	if err != nil {
		return err
	}

	log.Printf("Executing query: %s", query)

	// 执行查询
	rows, err := task.db.Raw(query).Rows()
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	// 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %v", err)
	}

	// 创建扫描目标
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for idx := range values {
		valuePtrs[idx] = &values[idx]
	}

	recordCount := 0
	for rows.Next() {
		// 扫描行数据
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		// 创建记录
		record := element.NewRecord()
		for _, value := range values {
			column := task.convertToColumn(value)
			record.AddColumn(column)
		}

		// 发送记录
		if err := recordSender.SendRecord(record); err != nil {
			return fmt.Errorf("failed to send record: %v", err)
		}

		recordCount++
		if recordCount%1000 == 0 {
			log.Printf("Read %d records", recordCount)
		}
	}

	log.Printf("Total records read: %d", recordCount)
	return nil
}

func (task *MySQLReaderTask) buildQuery() (string, error) {
	// 构建SELECT语句
	columns := task.readerJob.columns
	table := task.readerJob.tables[0]

	var columnStr string
	if len(columns) == 1 && columns[0] == "*" {
		columnStr = "*"
	} else {
		// MySQL使用反引号包围列名和表名
		quotedColumns := make([]string, len(columns))
		for i, col := range columns {
			quotedColumns[i] = fmt.Sprintf("`%s`", col)
		}
		columnStr = strings.Join(quotedColumns, ", ")
	}

	query := fmt.Sprintf("SELECT %s FROM `%s`", columnStr, table)

	// 添加WHERE条件
	var whereConditions []string

	// 原始WHERE条件
	if task.readerJob.where != "" {
		whereConditions = append(whereConditions, task.readerJob.where)
	}

	// 分片范围条件
	if splitRange := task.config.Get("parameter.splitRange"); splitRange != nil {
		if rangeMap, ok := splitRange.(map[string]interface{}); ok {
			start := rangeMap["start"]
			end := rangeMap["end"]
			condition := fmt.Sprintf("`%s` >= %v AND `%s` <= %v",
				task.readerJob.splitPk, start, task.readerJob.splitPk, end)
			whereConditions = append(whereConditions, condition)
		}
	}

	if len(whereConditions) > 0 {
		query += " WHERE " + strings.Join(whereConditions, " AND ")
	}

	return query, nil
}

func (task *MySQLReaderTask) convertToColumn(value interface{}) element.Column {
	if value == nil {
		return element.NewStringColumn("")
	}

	switch v := value.(type) {
	case int64:
		return element.NewLongColumn(v)
	case int32:
		return element.NewLongColumn(int64(v))
	case int:
		return element.NewLongColumn(int64(v))
	case float64:
		return element.NewDoubleColumn(v)
	case float32:
		return element.NewDoubleColumn(float64(v))
	case string:
		return element.NewStringColumn(v)
	case []byte:
		return element.NewStringColumn(string(v))
	case time.Time:
		return element.NewDateColumn(v)
	case bool:
		return element.NewBoolColumn(v)
	default:
		// 其他类型转换为字符串
		return element.NewStringColumn(fmt.Sprintf("%v", v))
	}
}

func (task *MySQLReaderTask) Post() error {
	return nil
}

func (task *MySQLReaderTask) Destroy() error {
	if task.db != nil {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}
	return nil
}