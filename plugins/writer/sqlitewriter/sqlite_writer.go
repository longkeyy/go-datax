package sqlitewriter

import (
	"fmt"
	"log"
	"strings"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
	coreplugin "github.com/longkeyy/go-datax/core/registry"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	DefaultBatchSize = 1024
)

// SQLiteWriterJob SQLite写入作业
type SQLiteWriterJob struct {
	config    config.Configuration
	jdbcUrls  []string
	tables    []string
	columns   []string
	preSql    []string
	postSql   []string
	writeMode string
	batchSize int
	factory   *factory.DataXFactory
}

func NewSQLiteWriterJob() *SQLiteWriterJob {
	return &SQLiteWriterJob{
		factory: factory.GetGlobalFactory(),
	}
}

func (job *SQLiteWriterJob) Init(config config.Configuration) error {
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

	// 如果配置为*，需要获取表的实际列名
	if len(job.columns) == 1 && job.columns[0] == "*" {
		actualColumns, err := job.getTableColumns()
		if err != nil {
			log.Printf("Warning: failed to get table columns, will use * as is: %v", err)
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

	job.batchSize = config.GetInt("parameter.batchSize")
	if job.batchSize <= 0 {
		job.batchSize = DefaultBatchSize
	}

	log.Printf("SQLite Writer initialized: tables=%v, columns=%v, writeMode=%s, batchSize=%d",
		job.tables, job.columns, job.writeMode, job.batchSize)
	return nil
}

func (job *SQLiteWriterJob) Prepare() error {
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
			log.Printf("Executing pre SQL: %s", sql)
			if err := db.Exec(sql).Error; err != nil {
				return fmt.Errorf("failed to execute pre SQL: %v", err)
			}
		}
	}
	return nil
}

func (job *SQLiteWriterJob) getTableColumns() ([]string, error) {
	db, err := job.connect()
	if err != nil {
		return nil, err
	}
	defer func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	// 查询表的列信息 (SQLite使用pragma)
	query := fmt.Sprintf("PRAGMA table_info(%s)", job.tables[0])

	rows, err := db.Raw(query).Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to query table columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var cid int
		var columnName, dataType string
		var notNull, pk int
		var defaultValue interface{}

		if err := rows.Scan(&cid, &columnName, &dataType, &notNull, &defaultValue, &pk); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %v", err)
		}
		columns = append(columns, columnName)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %s", job.tables[0])
	}

	log.Printf("Retrieved table columns for %s: %v", job.tables[0], columns)
	return columns, nil
}

func (job *SQLiteWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	taskConfigs := make([]config.Configuration, 0)

	// SQLite Writer通常不需要分片，每个task写入相同的表
	for i := 0; i < mandatoryNumber; i++ {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	log.Printf("Split into %d SQLite writer tasks", len(taskConfigs))
	return taskConfigs, nil
}

func (job *SQLiteWriterJob) connect() (*gorm.DB, error) {
	// 构建连接字符串
	jdbcUrl := job.jdbcUrls[0]

	// 转换JDBC URL为SQLite连接字符串
	dbPath, err := job.convertJdbcUrl(jdbcUrl)
	if err != nil {
		return nil, err
	}

	// 连接数据库
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	return db, nil
}

func (job *SQLiteWriterJob) convertJdbcUrl(jdbcUrl string) (string, error) {
	// 解析JDBC URL: jdbc:sqlite:path/to/database.db
	if !strings.HasPrefix(jdbcUrl, "jdbc:sqlite:") {
		return "", fmt.Errorf("invalid SQLite JDBC URL: %s", jdbcUrl)
	}

	// 移除jdbc:sqlite:前缀，获取数据库文件路径
	dbPath := strings.TrimPrefix(jdbcUrl, "jdbc:sqlite:")

	return dbPath, nil
}

func (job *SQLiteWriterJob) Post() error {
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
			log.Printf("Executing post SQL: %s", sql)
			if err := db.Exec(sql).Error; err != nil {
				return fmt.Errorf("failed to execute post SQL: %v", err)
			}
		}
	}
	return nil
}

func (job *SQLiteWriterJob) Destroy() error {
	return nil
}

// SQLiteWriterTask SQLite写入任务
type SQLiteWriterTask struct {
	config    config.Configuration
	writerJob *SQLiteWriterJob
	db        *gorm.DB
	records   []element.Record
	factory   *factory.DataXFactory
}

func NewSQLiteWriterTask() *SQLiteWriterTask {
	return &SQLiteWriterTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *SQLiteWriterTask) Init(config config.Configuration) error {
	task.config = config

	// 创建WriterJob来重用连接逻辑
	task.writerJob = NewSQLiteWriterJob()
	err := task.writerJob.Init(config)
	if err != nil {
		return err
	}

	// 建立数据库连接
	task.db, err = task.writerJob.connect()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	// 初始化记录缓冲区
	task.records = make([]element.Record, 0, task.writerJob.batchSize)

	return nil
}

func (task *SQLiteWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	defer func() {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
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

		// 添加记录到缓冲区
		task.records = append(task.records, record)

		// 检查是否需要批量写入
		if len(task.records) >= task.writerJob.batchSize {
			if err := task.flushRecords(); err != nil {
				return err
			}
			recordCount += len(task.records)
			task.records = task.records[:0] // 清空缓冲区
		}
	}

	// 写入剩余记录
	if len(task.records) > 0 {
		if err := task.flushRecords(); err != nil {
			return err
		}
		recordCount += len(task.records)
	}

	log.Printf("Total records written: %d", recordCount)
	return nil
}

func (task *SQLiteWriterTask) flushRecords() error {
	if len(task.records) == 0 {
		return nil
	}

	switch task.writerJob.writeMode {
	case "insert":
		return task.insertRecords()
	case "replace":
		return task.replaceRecords()
	case "update":
		return task.updateRecords()
	default:
		return fmt.Errorf("unsupported write mode: %s", task.writerJob.writeMode)
	}
}

func (task *SQLiteWriterTask) insertRecords() error {
	table := task.writerJob.tables[0]
	columns := task.writerJob.columns

	// 构建INSERT语句
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	// sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
	//	table,
	//	strings.Join(columns, ", "),
	//	strings.Join(placeholders, ", "))

	// 准备批量数据
	values := make([]interface{}, 0, len(task.records)*len(columns))
	for _, record := range task.records {
		for i := range columns {
			if i < record.GetColumnNumber() {
				values = append(values, task.convertColumnValue(record.GetColumn(i)))
			} else {
				values = append(values, nil)
			}
		}
	}

	// 构建批量INSERT语句
	valueClause := "(" + strings.Join(placeholders, ", ") + ")"
	valueClauses := make([]string, len(task.records))
	for i := range valueClauses {
		valueClauses[i] = valueClause
	}

	batchSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		table,
		strings.Join(columns, ", "),
		strings.Join(valueClauses, ", "))

	// 执行批量插入
	if err := task.db.Exec(batchSQL, values...).Error; err != nil {
		return fmt.Errorf("failed to insert records: %v", err)
	}

	return nil
}

func (task *SQLiteWriterTask) replaceRecords() error {
	table := task.writerJob.tables[0]
	columns := task.writerJob.columns

	// 构建REPLACE语句
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	// 准备批量数据
	values := make([]interface{}, 0, len(task.records)*len(columns))
	for _, record := range task.records {
		for i := range columns {
			if i < record.GetColumnNumber() {
				values = append(values, task.convertColumnValue(record.GetColumn(i)))
			} else {
				values = append(values, nil)
			}
		}
	}

	// 构建批量REPLACE语句
	valueClause := "(" + strings.Join(placeholders, ", ") + ")"
	valueClauses := make([]string, len(task.records))
	for i := range valueClauses {
		valueClauses[i] = valueClause
	}

	batchSQL := fmt.Sprintf("REPLACE INTO %s (%s) VALUES %s",
		table,
		strings.Join(columns, ", "),
		strings.Join(valueClauses, ", "))

	// 执行批量替换
	if err := task.db.Exec(batchSQL, values...).Error; err != nil {
		return fmt.Errorf("failed to replace records: %v", err)
	}

	return nil
}

func (task *SQLiteWriterTask) updateRecords() error {
	// UPDATE模式需要配置主键，这里简化实现
	return fmt.Errorf("UPDATE mode is not implemented yet")
}

func (task *SQLiteWriterTask) convertColumnValue(column element.Column) interface{} {
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
		return val
	case element.TypeBool:
		val, _ := column.GetAsBool()
		return val
	case element.TypeBytes:
		val, _ := column.GetAsBytes()
		return val
	default:
		// 其他类型作为字符串处理
		return column.GetAsString()
	}
}

func (task *SQLiteWriterTask) Prepare() error {
	return nil
}

func (task *SQLiteWriterTask) Post() error {
	return nil
}

func (task *SQLiteWriterTask) Destroy() error {
	if task.db != nil {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}
	return nil
}