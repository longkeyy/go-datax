package postgresqlwriter

import (
	"fmt"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"log"
	"strings"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	DefaultBatchSize = 1024
)

// PostgreSQLWriterJob PostgreSQL写入作业
type PostgreSQLWriterJob struct {
	config   *config.Configuration
	username string
	password string
	jdbcUrl  string
	table    string
	columns  []string
	preSql   []string
	postSql  []string
}

func NewPostgreSQLWriterJob() *PostgreSQLWriterJob {
	return &PostgreSQLWriterJob{}
}

func (job *PostgreSQLWriterJob) Init(config *config.Configuration) error {
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
	tables := conn.GetStringList("table")

	if job.jdbcUrl == "" || len(tables) == 0 {
		return fmt.Errorf("jdbcUrl and table are required")
	}

	job.table = tables[0]

	// 获取列信息
	job.columns = config.GetStringList("parameter.column")
	if len(job.columns) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	// 获取预处理SQL
	job.preSql = config.GetStringList("parameter.preSql")
	job.postSql = config.GetStringList("parameter.postSql")

	log.Printf("PostgreSQL Writer initialized: table=%s, columns=%v", job.table, job.columns)
	return nil
}

func (job *PostgreSQLWriterJob) Split(mandatoryNumber int) ([]*config.Configuration, error) {
	taskConfigs := make([]*config.Configuration, mandatoryNumber)

	for i := 0; i < mandatoryNumber; i++ {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfigs[i] = taskConfig
	}

	log.Printf("Split into %d writer tasks", mandatoryNumber)
	return taskConfigs, nil
}

func (job *PostgreSQLWriterJob) Prepare() error {
	// 执行preSql
	if len(job.preSql) > 0 {
		db, err := job.connect()
		if err != nil {
			return fmt.Errorf("failed to connect for prepare: %v", err)
		}
		defer func() {
			if sqlDB, err := db.DB(); err == nil {
				sqlDB.Close()
			}
		}()

		for _, sql := range job.preSql {
			log.Printf("Executing preSql: %s", sql)
			if err := db.Exec(sql).Error; err != nil {
				return fmt.Errorf("failed to execute preSql [%s]: %v", sql, err)
			}
		}
	}

	return nil
}

func (job *PostgreSQLWriterJob) connect() (*gorm.DB, error) {
	// 转换JDBC URL为PostgreSQL连接字符串
	dsn, err := job.convertJdbcUrl(job.jdbcUrl)
	if err != nil {
		return nil, err
	}

	// 连接数据库
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	return db, nil
}

func (job *PostgreSQLWriterJob) convertJdbcUrl(jdbcUrl string) (string, error) {
	// 解析JDBC URL: jdbc:postgresql://host:port/database
	if !strings.HasPrefix(jdbcUrl, "jdbc:postgresql://") {
		return "", fmt.Errorf("invalid PostgreSQL JDBC URL: %s", jdbcUrl)
	}

	// 移除jdbc:postgresql://前缀
	url := strings.TrimPrefix(jdbcUrl, "jdbc:postgresql://")

	// 分离host:port和database
	parts := strings.Split(url, "/")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid JDBC URL format: %s", jdbcUrl)
	}

	hostPort := parts[0]
	database := parts[1]

	// 构建PostgreSQL连接字符串
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
		strings.Replace(hostPort, ":", " port=", 1),
		job.username,
		job.password,
		database)

	return dsn, nil
}

func (job *PostgreSQLWriterJob) Post() error {
	// 执行postSql
	if len(job.postSql) > 0 {
		db, err := job.connect()
		if err != nil {
			return fmt.Errorf("failed to connect for post: %v", err)
		}
		defer func() {
			if sqlDB, err := db.DB(); err == nil {
				sqlDB.Close()
			}
		}()

		for _, sql := range job.postSql {
			log.Printf("Executing postSql: %s", sql)
			if err := db.Exec(sql).Error; err != nil {
				return fmt.Errorf("failed to execute postSql [%s]: %v", sql, err)
			}
		}
	}

	return nil
}

func (job *PostgreSQLWriterJob) Destroy() error {
	return nil
}

// PostgreSQLWriterTask PostgreSQL写入任务
type PostgreSQLWriterTask struct {
	config    *config.Configuration
	writerJob *PostgreSQLWriterJob
	db        *gorm.DB
	batchSize int
}

func NewPostgreSQLWriterTask() *PostgreSQLWriterTask {
	return &PostgreSQLWriterTask{}
}

func (task *PostgreSQLWriterTask) Init(config *config.Configuration) error {
	task.config = config
	task.batchSize = config.GetIntWithDefault("parameter.batchSize", DefaultBatchSize)

	// 创建WriterJob来重用连接逻辑
	task.writerJob = NewPostgreSQLWriterJob()
	err := task.writerJob.Init(config)
	if err != nil {
		return err
	}

	// 建立数据库连接
	task.db, err = task.writerJob.connect()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	return nil
}

func (task *PostgreSQLWriterTask) Prepare() error {
	return nil
}

func (task *PostgreSQLWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	defer func() {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	// 准备批量插入
	batch := make([]element.Record, 0, task.batchSize)
	totalCount := 0

	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == plugin.ErrChannelClosed {
				// 处理最后一批数据
				if len(batch) > 0 {
					if err := task.writeBatch(batch); err != nil {
						return err
					}
					totalCount += len(batch)
				}
				break
			}
			return fmt.Errorf("failed to receive record: %v", err)
		}

		batch = append(batch, record)

		// 达到批次大小时写入
		if len(batch) >= task.batchSize {
			if err := task.writeBatch(batch); err != nil {
				return err
			}
			totalCount += len(batch)
			batch = batch[:0] // 清空batch
		}
	}

	log.Printf("Total records written: %d", totalCount)
	return nil
}

func (task *PostgreSQLWriterTask) writeBatch(records []element.Record) error {
	if len(records) == 0 {
		return nil
	}

	// 构建批量插入SQL
	insertSQL, values, err := task.buildBatchInsertSQL(records)
	if err != nil {
		return err
	}

	// 执行批量插入
	if err := task.db.Exec(insertSQL, values...).Error; err != nil {
		return fmt.Errorf("failed to execute batch insert: %v", err)
	}

	log.Printf("Written batch of %d records", len(records))
	return nil
}

func (task *PostgreSQLWriterTask) buildBatchInsertSQL(records []element.Record) (string, []interface{}, error) {
	table := task.writerJob.table
	columns := task.writerJob.columns

	// 构建INSERT语句头部
	var columnStr string
	if len(columns) == 1 && columns[0] == "*" {
		// 如果是*，需要从记录中推断列数
		columnCount := records[0].GetColumnNumber()
		columnNames := make([]string, columnCount)
		for i := 0; i < columnCount; i++ {
			columnNames[i] = fmt.Sprintf("col_%d", i+1)
		}
		columnStr = strings.Join(columnNames, ", ")
	} else {
		columnStr = strings.Join(columns, ", ")
	}

	// 构建VALUES部分
	valuePlaceholders := make([]string, len(records))
	allValues := make([]interface{}, 0, len(records)*len(columns))
	placeholderIndex := 1

	for i, record := range records {
		columnCount := record.GetColumnNumber()
		if len(columns) != 1 || columns[0] != "*" {
			if columnCount != len(columns) {
				return "", nil, fmt.Errorf("record column count (%d) doesn't match config columns (%d)",
					columnCount, len(columns))
			}
		}

		// 为每个记录构建占位符
		recordPlaceholders := make([]string, columnCount)
		for j := 0; j < columnCount; j++ {
			recordPlaceholders[j] = fmt.Sprintf("$%d", placeholderIndex)
			placeholderIndex++

			// 获取列值
			column := record.GetColumn(j)
			value := task.convertColumnToValue(column)
			allValues = append(allValues, value)
		}

		valuePlaceholders[i] = fmt.Sprintf("(%s)", strings.Join(recordPlaceholders, ", "))
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		table, columnStr, strings.Join(valuePlaceholders, ", "))

	return insertSQL, allValues, nil
}

func (task *PostgreSQLWriterTask) convertColumnToValue(column element.Column) interface{} {
	if column.IsNull() {
		return nil
	}

	switch column.GetType() {
	case element.TypeLong:
		if value, err := column.GetAsLong(); err == nil {
			return value
		}
	case element.TypeDouble:
		if value, err := column.GetAsDouble(); err == nil {
			return value
		}
	case element.TypeString:
		return column.GetAsString()
	case element.TypeDate:
		if value, err := column.GetAsDate(); err == nil {
			return value
		}
	case element.TypeBool:
		if value, err := column.GetAsBool(); err == nil {
			return value
		}
	case element.TypeBytes:
		if value, err := column.GetAsBytes(); err == nil {
			return value
		}
	}

	// 默认转换为字符串
	return column.GetAsString()
}

func (task *PostgreSQLWriterTask) Post() error {
	return nil
}

func (task *PostgreSQLWriterTask) Destroy() error {
	if task.db != nil {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}
	return nil
}