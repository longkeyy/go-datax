package sqlserverwriter

import (
	"fmt"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"log"
	"strings"

	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	DefaultBatchSize = 1024
)

// SQLServerWriterJob SQL Server写入作业
type SQLServerWriterJob struct {
	config    *config.Configuration
	username  string
	password  string
	jdbcUrl   string
	tables    []string
	columns   []string
	preSql    []string
	postSql   []string
	session   []string
	batchSize int
}

func NewSQLServerWriterJob() *SQLServerWriterJob {
	return &SQLServerWriterJob{
		batchSize: DefaultBatchSize,
	}
}

func (job *SQLServerWriterJob) Init(config *config.Configuration) error {
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
	job.jdbcUrl = conn.GetString("jdbcUrl")
	job.tables = conn.GetStringList("table")

	if job.jdbcUrl == "" {
		return fmt.Errorf("jdbcUrl is required")
	}
	if len(job.tables) == 0 {
		return fmt.Errorf("table is required")
	}

	// 获取列信息
	job.columns = config.GetStringList("parameter.column")
	if len(job.columns) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	// 获取可选参数
	job.preSql = config.GetStringList("parameter.preSql")
	job.postSql = config.GetStringList("parameter.postSql")
	job.session = config.GetStringList("parameter.session")

	// 获取batchSize
	if batchSize := config.GetInt("parameter.batchSize"); batchSize > 0 {
		job.batchSize = batchSize
	}

	log.Printf("SQL Server Writer initialized: tables=%v, columns=%v, batchSize=%d", job.tables, job.columns, job.batchSize)
	return nil
}

func (job *SQLServerWriterJob) Prepare() error {
	if len(job.preSql) == 0 {
		return nil
	}

	db, err := job.connect()
	if err != nil {
		return fmt.Errorf("failed to connect for prepare: %v", err)
	}
	defer func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	// 执行session配置
	if err := job.executeSession(db); err != nil {
		return fmt.Errorf("failed to execute session: %v", err)
	}

	// 执行preSql
	for _, table := range job.tables {
		for _, sql := range job.preSql {
			// 替换@table变量
			actualSQL := strings.ReplaceAll(sql, "@table", table)
			log.Printf("Executing preSql: %s", actualSQL)
			if err := db.Exec(actualSQL).Error; err != nil {
				return fmt.Errorf("failed to execute preSql %s: %v", actualSQL, err)
			}
		}
	}

	return nil
}

func (job *SQLServerWriterJob) Split(adviceNumber int) ([]*config.Configuration, error) {
	taskConfigs := make([]*config.Configuration, 0)

	// 为每个表创建一个任务配置
	for i, table := range job.tables {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfig.Set("parameter.table", table)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	log.Printf("Split into %d tasks for tables: %v", len(taskConfigs), job.tables)
	return taskConfigs, nil
}

func (job *SQLServerWriterJob) Post() error {
	if len(job.postSql) == 0 {
		return nil
	}

	db, err := job.connect()
	if err != nil {
		return fmt.Errorf("failed to connect for post: %v", err)
	}
	defer func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	// 执行postSql
	for _, table := range job.tables {
		for _, sql := range job.postSql {
			// 替换@table变量
			actualSQL := strings.ReplaceAll(sql, "@table", table)
			log.Printf("Executing postSql: %s", actualSQL)
			if err := db.Exec(actualSQL).Error; err != nil {
				return fmt.Errorf("failed to execute postSql %s: %v", actualSQL, err)
			}
		}
	}

	return nil
}

func (job *SQLServerWriterJob) Destroy() error {
	return nil
}

func (job *SQLServerWriterJob) connect() (*gorm.DB, error) {
	// 转换JDBC URL为SQL Server连接字符串
	dsn, err := job.convertJdbcUrl(job.jdbcUrl)
	if err != nil {
		return nil, err
	}

	// 连接数据库
	db, err := gorm.Open(sqlserver.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	return db, nil
}

func (job *SQLServerWriterJob) convertJdbcUrl(jdbcUrl string) (string, error) {
	// 解析JDBC URL: jdbc:sqlserver://host:port;DatabaseName=database
	if !strings.HasPrefix(jdbcUrl, "jdbc:sqlserver://") {
		return "", fmt.Errorf("invalid SQL Server JDBC URL: %s", jdbcUrl)
	}

	// 移除jdbc:sqlserver://前缀
	url := strings.TrimPrefix(jdbcUrl, "jdbc:sqlserver://")

	// 分离host:port和参数
	parts := strings.Split(url, ";")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid JDBC URL format: %s", jdbcUrl)
	}

	hostPort := parts[0]

	// 解析参数
	params := make(map[string]string)
	for i := 1; i < len(parts); i++ {
		if strings.Contains(parts[i], "=") {
			kv := strings.Split(parts[i], "=")
			if len(kv) == 2 {
				params[kv[0]] = kv[1]
			}
		}
	}

	// 获取数据库名
	database, exists := params["DatabaseName"]
	if !exists || database == "" {
		return "", fmt.Errorf("DatabaseName is required in JDBC URL")
	}

	// 构建SQL Server DSN字符串
	dsn := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s",
		job.username,
		job.password,
		hostPort,
		database)

	// 添加其他参数
	for key, value := range params {
		if key != "DatabaseName" {
			dsn += fmt.Sprintf("&%s=%s", key, value)
		}
	}

	return dsn, nil
}

func (job *SQLServerWriterJob) executeSession(db *gorm.DB) error {
	for _, sql := range job.session {
		log.Printf("Executing session SQL: %s", sql)
		if err := db.Exec(sql).Error; err != nil {
			return fmt.Errorf("failed to execute session SQL %s: %v", sql, err)
		}
	}
	return nil
}

func (job *SQLServerWriterJob) getTableColumns(db *gorm.DB, tableName string) ([]string, error) {
	var columns []string

	// 查询SQL Server系统表获取列信息
	query := `
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`

	rows, err := db.Raw(query, tableName).Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to query table columns: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan column name: %v", err)
		}
		columns = append(columns, columnName)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %s", tableName)
	}

	return columns, nil
}

// SQLServerWriterTask SQL Server写入任务
type SQLServerWriterTask struct {
	config    *config.Configuration
	writerJob *SQLServerWriterJob
	db        *gorm.DB
	tableName string
	columns   []string
	records   []element.Record
}

func NewSQLServerWriterTask() *SQLServerWriterTask {
	return &SQLServerWriterTask{
		records: make([]element.Record, 0),
	}
}

func (task *SQLServerWriterTask) Init(config *config.Configuration) error {
	task.config = config

	// 创建WriterJob来重用连接逻辑
	task.writerJob = NewSQLServerWriterJob()
	err := task.writerJob.Init(config)
	if err != nil {
		return err
	}

	// 获取当前任务的表名
	task.tableName = config.GetString("parameter.table")
	if task.tableName == "" {
		// 如果没有设置，使用第一个表
		if len(task.writerJob.tables) > 0 {
			task.tableName = task.writerJob.tables[0]
		} else {
			return fmt.Errorf("no table specified for task")
		}
	}

	// 建立数据库连接
	task.db, err = task.writerJob.connect()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	// 执行session配置
	if err := task.writerJob.executeSession(task.db); err != nil {
		return fmt.Errorf("failed to execute session: %v", err)
	}

	// 解析列配置
	task.columns = task.writerJob.columns
	if len(task.columns) == 1 && task.columns[0] == "*" {
		// 获取表的所有列
		actualColumns, err := task.writerJob.getTableColumns(task.db, task.tableName)
		if err != nil {
			return fmt.Errorf("failed to get table columns: %v", err)
		}
		task.columns = actualColumns
		log.Printf("Using all columns for table %s: %v", task.tableName, task.columns)
	}

	return nil
}

func (task *SQLServerWriterTask) Prepare() error {
	return nil
}

func (task *SQLServerWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	defer func() {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == plugin.ErrChannelClosed {
				break
			}
			return fmt.Errorf("failed to receive record: %v", err)
		}

		task.records = append(task.records, record)

		// 批量插入
		if len(task.records) >= task.writerJob.batchSize {
			if err := task.flushRecords(); err != nil {
				return err
			}
		}
	}

	// 插入剩余记录
	if len(task.records) > 0 {
		if err := task.flushRecords(); err != nil {
			return err
		}
	}

	return nil
}

func (task *SQLServerWriterTask) flushRecords() error {
	if len(task.records) == 0 {
		return nil
	}

	// 构建批量插入SQL
	if err := task.insertRecords(); err != nil {
		return fmt.Errorf("failed to insert records: %v", err)
	}

	// 清空缓存
	task.records = task.records[:0]
	log.Printf("Flushed %d records to table %s", len(task.records), task.tableName)

	return nil
}

func (task *SQLServerWriterTask) insertRecords() error {
	if len(task.records) == 0 {
		return nil
	}

	// 构建INSERT语句
	placeholders := make([]string, len(task.records))
	values := make([]interface{}, 0, len(task.records)*len(task.columns))

	for i := range task.records {
		columnPlaceholders := make([]string, len(task.columns))
		for j := range task.columns {
			columnPlaceholders[j] = "?"
		}
		placeholders[i] = fmt.Sprintf("(%s)", strings.Join(columnPlaceholders, ", "))

		// 添加记录的值
		for j := range task.columns {
			if j < task.records[i].GetColumnNumber() {
				value := task.convertColumnValue(task.records[i].GetColumn(j))
				values = append(values, value)
			} else {
				values = append(values, nil)
			}
		}
	}

	// 构建列名
	quotedColumns := make([]string, len(task.columns))
	for i, col := range task.columns {
		quotedColumns[i] = fmt.Sprintf("[%s]", col)
	}

	// 执行批量插入
	sql := fmt.Sprintf("INSERT INTO [%s] (%s) VALUES %s",
		task.tableName,
		strings.Join(quotedColumns, ", "),
		strings.Join(placeholders, ", "))

	log.Printf("Executing batch insert with %d records", len(task.records))
	if err := task.db.Exec(sql, values...).Error; err != nil {
		return fmt.Errorf("failed to execute batch insert: %v", err)
	}

	return nil
}

func (task *SQLServerWriterTask) convertColumnValue(column element.Column) interface{} {
	if column == nil {
		return nil
	}

	switch column.GetType() {
	case element.TypeLong:
		if longCol, ok := column.(*element.LongColumn); ok {
			if val, err := longCol.GetAsLong(); err == nil {
				return val
			}
		}
	case element.TypeDouble:
		if doubleCol, ok := column.(*element.DoubleColumn); ok {
			if val, err := doubleCol.GetAsDouble(); err == nil {
				return val
			}
		}
	case element.TypeString:
		if stringCol, ok := column.(*element.StringColumn); ok {
			return stringCol.GetAsString()
		}
	case element.TypeDate:
		if dateCol, ok := column.(*element.DateColumn); ok {
			if val, err := dateCol.GetAsDate(); err == nil {
				return val
			}
		}
	case element.TypeBool:
		if boolCol, ok := column.(*element.BoolColumn); ok {
			if val, err := boolCol.GetAsBool(); err == nil {
				return val
			}
		}
	case element.TypeBytes:
		if bytesCol, ok := column.(*element.BytesColumn); ok {
			if val, err := bytesCol.GetAsBytes(); err == nil {
				return val
			}
		}
	}

	return column.GetAsString()
}

func (task *SQLServerWriterTask) Post() error {
	return nil
}

func (task *SQLServerWriterTask) Destroy() error {
	if task.db != nil {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}
	return nil
}