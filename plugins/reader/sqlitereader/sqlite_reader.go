package sqlitereader

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/factory"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
)

const (
	DefaultFetchSize = 1024
)

// SQLiteReaderJob SQLite读取作业
type SQLiteReaderJob struct {
	config   config.Configuration
	jdbcUrls []string
	tables   []string
	columns  []string
	where    string
	splitPk  string
	factory  *factory.DataXFactory
}

func NewSQLiteReaderJob() *SQLiteReaderJob {
	return &SQLiteReaderJob{
		factory: factory.GetGlobalFactory(),
	}
}

func (job *SQLiteReaderJob) Init(config config.Configuration) error {
	job.config = config

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

	logger.Component().WithComponent("SQLiteReader").Info("SQLite Reader initialized",
		zap.Any("tables", job.tables),
		zap.Any("columns", job.columns))
	return nil
}

func (job *SQLiteReaderJob) Split(adviceNumber int) ([]config.Configuration, error) {
	taskConfigs := make([]config.Configuration, 0)

	// 如果没有设置splitPk，则不进行分片，使用单个任务
	if job.splitPk == "" {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", 0)
		taskConfigs = append(taskConfigs, taskConfig)
		logger.Component().WithComponent("SQLiteReader").Info("No splitPk specified, using single task")
		return taskConfigs, nil
	}

	// 如果设置了splitPk，尝试进行分片
	ranges, err := job.calculateSplitRanges(adviceNumber)
	if err != nil {
		logger.Component().WithComponent("SQLiteReader").Warn("Failed to calculate split ranges, fallback to single task",
			zap.Error(err))
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

	logger.Component().WithComponent("SQLiteReader").Info("Split into tasks based on splitPk",
		zap.Int("taskCount", len(taskConfigs)),
		zap.String("splitPk", job.splitPk))
	return taskConfigs, nil
}

func (job *SQLiteReaderJob) calculateSplitRanges(adviceNumber int) ([]map[string]interface{}, error) {
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
	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", job.splitPk, job.splitPk, table)
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

func (job *SQLiteReaderJob) connect() (*gorm.DB, error) {
	// 构建连接字符串
	jdbcUrl := job.jdbcUrls[0]

	// 转换JDBC URL为SQLite连接字符串
	dbPath, err := job.convertJdbcUrl(jdbcUrl)
	if err != nil {
		return nil, err
	}

	// 连接数据库
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		Logger: gormLogger.Default.LogMode(gormLogger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	return db, nil
}

func (job *SQLiteReaderJob) convertJdbcUrl(jdbcUrl string) (string, error) {
	// 解析JDBC URL: jdbc:sqlite:path/to/database.db
	if !strings.HasPrefix(jdbcUrl, "jdbc:sqlite:") {
		return "", fmt.Errorf("invalid SQLite JDBC URL: %s", jdbcUrl)
	}

	// 移除jdbc:sqlite:前缀，获取数据库文件路径
	dbPath := strings.TrimPrefix(jdbcUrl, "jdbc:sqlite:")

	return dbPath, nil
}

func (job *SQLiteReaderJob) Post() error {
	return nil
}

func (job *SQLiteReaderJob) Destroy() error {
	return nil
}

// SQLiteReaderTask SQLite读取任务
type SQLiteReaderTask struct {
	config    config.Configuration
	readerJob *SQLiteReaderJob
	db        *gorm.DB
	factory   *factory.DataXFactory
}

func NewSQLiteReaderTask() *SQLiteReaderTask {
	return &SQLiteReaderTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *SQLiteReaderTask) Init(config config.Configuration) error {
	task.config = config

	// 创建ReaderJob来重用连接逻辑
	task.readerJob = NewSQLiteReaderJob()
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

func (task *SQLiteReaderTask) StartRead(recordSender plugin.RecordSender) error {
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

	logger.Component().WithComponent("SQLiteReader").Info("Executing query",
		zap.String("query", query))

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
		record := task.factory.GetRecordFactory().CreateRecord()
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
			logger.Component().WithComponent("SQLiteReader").Info("Reading progress",
				zap.Int("recordCount", recordCount))
		}
	}

	logger.Component().WithComponent("SQLiteReader").Info("Reading completed",
		zap.Int("totalRecords", recordCount))
	return nil
}

func (task *SQLiteReaderTask) buildQuery() (string, error) {
	// 构建SELECT语句
	columns := task.readerJob.columns
	table := task.readerJob.tables[0]

	var columnStr string
	if len(columns) == 1 && columns[0] == "*" {
		columnStr = "*"
	} else {
		columnStr = strings.Join(columns, ", ")
	}

	query := fmt.Sprintf("SELECT %s FROM %s", columnStr, table)

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
			condition := fmt.Sprintf("%s >= %v AND %s <= %v",
				task.readerJob.splitPk, start, task.readerJob.splitPk, end)
			whereConditions = append(whereConditions, condition)
		}
	}

	if len(whereConditions) > 0 {
		query += " WHERE " + strings.Join(whereConditions, " AND ")
	}

	return query, nil
}

func (task *SQLiteReaderTask) convertToColumn(value interface{}) element.Column {
	columnFactory := task.factory.GetColumnFactory()
	if value == nil {
		return columnFactory.CreateStringColumn("")
	}

	switch v := value.(type) {
	case int64:
		return columnFactory.CreateLongColumn(v)
	case int32:
		return columnFactory.CreateLongColumn(int64(v))
	case int:
		return columnFactory.CreateLongColumn(int64(v))
	case float64:
		return columnFactory.CreateDoubleColumn(v)
	case float32:
		return columnFactory.CreateDoubleColumn(float64(v))
	case string:
		return columnFactory.CreateStringColumn(v)
	case []byte:
		return columnFactory.CreateStringColumn(string(v))
	case time.Time:
		return columnFactory.CreateDateColumn(v)
	case bool:
		return columnFactory.CreateBoolColumn(v)
	default:
		// 其他类型转换为字符串
		return columnFactory.CreateStringColumn(fmt.Sprintf("%v", v))
	}
}

func (task *SQLiteReaderTask) Post() error {
	return nil
}

func (task *SQLiteReaderTask) Destroy() error {
	if task.db != nil {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}
	return nil
}