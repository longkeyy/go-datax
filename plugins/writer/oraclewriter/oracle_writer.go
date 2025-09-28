package oraclewriter

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/plugin"
	_ "github.com/sijms/go-ora/v2"
	"go.uber.org/zap"
)

const (
	DefaultBatchSize     = 1024
	DefaultInsertTimeout = 30 // seconds
)

// OracleWriterJob Oracle写入Job
type OracleWriterJob struct {
	username     string
	password     string
	jdbcUrl      string
	tables       []string
	columns      []string
	writeMode    string
	preSql       []string
	postSql      []string
	session      []string
	batchSize    int
	configuration *config.Configuration
}

// OracleWriterTask Oracle写入Task
type OracleWriterTask struct {
	writerJob     *OracleWriterJob
	config        *config.Configuration
	db            *sql.DB
	table         string
	insertStmt    *sql.Stmt
	columns       []string
	batchSize     int
	insertTimeout int
}

func NewOracleWriterJob() *OracleWriterJob {
	return &OracleWriterJob{}
}

func NewOracleWriterTask() *OracleWriterTask {
	return &OracleWriterTask{}
}

func (job *OracleWriterJob) Init(config *config.Configuration) error {
	compLogger := logger.Component().WithComponent("OracleWriterJob")
	compLogger.Info("Initializing Oracle writer job")

	job.configuration = config

	// 解析连接配置
	connections := config.GetListConfiguration("connection")
	if len(connections) == 0 {
		return fmt.Errorf("connection configuration is required")
	}

	// 使用第一个连接配置
	connection := connections[0]
	job.jdbcUrl = connection.GetString("jdbcUrl")
	if job.jdbcUrl == "" {
		return fmt.Errorf("jdbcUrl is required")
	}

	// 解析表配置
	job.tables = connection.GetStringList("table")
	if len(job.tables) == 0 {
		return fmt.Errorf("table configuration is required")
	}

	// 解析认证信息
	job.username = config.GetString("username")
	job.password = config.GetString("password")
	if job.username == "" || job.password == "" {
		return fmt.Errorf("username and password are required")
	}

	// 解析列配置
	job.columns = config.GetStringList("column")
	if len(job.columns) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	// 解析写入模式
	job.writeMode = config.GetStringWithDefault("writeMode", "INSERT")
	if job.writeMode != "INSERT" && job.writeMode != "UPDATE" && job.writeMode != "REPLACE" {
		return fmt.Errorf("unsupported writeMode: %s, only INSERT/UPDATE/REPLACE are supported", job.writeMode)
	}

	// 解析SQL配置
	job.preSql = config.GetStringList("preSql")
	job.postSql = config.GetStringList("postSql")
	job.session = config.GetStringList("session")

	// 解析批次大小
	job.batchSize = config.GetIntWithDefault("batchSize", DefaultBatchSize)

	compLogger.Info("Oracle writer job initialized",
		zap.String("jdbcUrl", job.jdbcUrl),
		zap.Strings("tables", job.tables),
		zap.Strings("columns", job.columns),
		zap.String("writeMode", job.writeMode),
		zap.Int("batchSize", job.batchSize))

	return nil
}

func (job *OracleWriterJob) Split(adviceNumber int) ([]*config.Configuration, error) {
	compLogger := logger.Component().WithComponent("OracleWriterJob")
	compLogger.Info("Splitting Oracle writer job", zap.Int("adviceNumber", adviceNumber))

	var taskConfigs []*config.Configuration

	// 为每个表创建一个任务配置
	for _, table := range job.tables {
		taskConfig := job.configuration.Clone()

		// 设置单表连接配置
		connectionConfig := map[string]interface{}{
			"jdbcUrl": job.jdbcUrl,
			"table":   []string{table},
		}
		taskConfig.Set("connection", []map[string]interface{}{connectionConfig})
		taskConfigs = append(taskConfigs, taskConfig)
	}

	compLogger.Info("Oracle writer job split completed", zap.Int("taskCount", len(taskConfigs)))
	return taskConfigs, nil
}

func (job *OracleWriterJob) Prepare() error {
	compLogger := logger.Component().WithComponent("OracleWriterJob")
	compLogger.Info("Preparing Oracle writer job")

	// 建立连接进行准备工作
	db, err := job.connect()
	if err != nil {
		return fmt.Errorf("failed to connect to Oracle database: %v", err)
	}
	defer db.Close()

	// 执行preSql
	for _, sql := range job.preSql {
		if sql != "" {
			compLogger.Info("Executing preSql", zap.String("sql", sql))
			if _, err := db.Exec(sql); err != nil {
				return fmt.Errorf("failed to execute preSql '%s': %v", sql, err)
			}
		}
	}

	compLogger.Info("Oracle writer job prepared successfully")
	return nil
}

func (job *OracleWriterJob) Post() error {
	compLogger := logger.Component().WithComponent("OracleWriterJob")
	compLogger.Info("Post-processing Oracle writer job")

	// 建立连接进行后处理工作
	db, err := job.connect()
	if err != nil {
		return fmt.Errorf("failed to connect to Oracle database: %v", err)
	}
	defer db.Close()

	// 执行postSql
	for _, sql := range job.postSql {
		if sql != "" {
			compLogger.Info("Executing postSql", zap.String("sql", sql))
			if _, err := db.Exec(sql); err != nil {
				return fmt.Errorf("failed to execute postSql '%s': %v", sql, err)
			}
		}
	}

	compLogger.Info("Oracle writer job post-processing completed")
	return nil
}

func (job *OracleWriterJob) Destroy() error {
	compLogger := logger.Component().WithComponent("OracleWriterJob")
	compLogger.Info("Destroying Oracle writer job")
	return nil
}

func (job *OracleWriterJob) connect() (*sql.DB, error) {
	// 解析Oracle连接字符串
	// 支持格式: oracle://host:port/service_name
	if !strings.HasPrefix(job.jdbcUrl, "oracle://") {
		return nil, fmt.Errorf("invalid Oracle JDBC URL format: %s", job.jdbcUrl)
	}

	urlPart := strings.TrimPrefix(job.jdbcUrl, "oracle://")

	// 构建go-ora连接字符串
	// 格式: oracle://user:password@host:port/service_name
	dsn := fmt.Sprintf("oracle://%s:%s@%s", job.username, job.password, urlPart)

	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open Oracle connection: %v", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping Oracle database: %v", err)
	}

	return db, nil
}

func (task *OracleWriterTask) Init(config *config.Configuration) error {
	compLogger := logger.Component().WithComponent("OracleWriterTask")
	compLogger.Info("Initializing Oracle writer task")

	task.config = config

	// 创建writerJob实例并初始化
	task.writerJob = NewOracleWriterJob()
	if err := task.writerJob.Init(config); err != nil {
		return err
	}

	// 获取表信息
	connections := config.GetListConfiguration("connection")
	if len(connections) > 0 {
		tables := connections[0].GetStringList("table")
		if len(tables) > 0 {
			task.table = tables[0]
		}
	}

	// 设置批次大小和超时时间
	task.batchSize = task.writerJob.batchSize
	task.insertTimeout = config.GetIntWithDefault("insertTimeout", DefaultInsertTimeout)

	// 建立数据库连接
	var err error
	task.db, err = task.writerJob.connect()
	if err != nil {
		return err
	}

	// 执行session SQL
	err = task.executeSessionSql()
	if err != nil {
		return err
	}

	// 准备插入语句
	err = task.prepareInsertStatement()
	if err != nil {
		return err
	}

	compLogger.Info("Oracle writer task initialized",
		zap.String("table", task.table),
		zap.Int("batchSize", task.batchSize))

	return nil
}

func (task *OracleWriterTask) executeSessionSql() error {
	for _, sessionSql := range task.writerJob.session {
		if sessionSql != "" {
			_, err := task.db.Exec(sessionSql)
			if err != nil {
				return fmt.Errorf("failed to execute session SQL '%s': %v", sessionSql, err)
			}
		}
	}
	return nil
}

func (task *OracleWriterTask) prepareInsertStatement() error {
	// 处理列配置
	columns := task.writerJob.columns
	if len(columns) == 1 && columns[0] == "*" {
		// 获取表的实际列名
		actualColumns, err := task.getTableColumns()
		if err != nil {
			return fmt.Errorf("failed to get table columns: %v", err)
		}
		columns = actualColumns
	}
	task.columns = columns

	// 构建INSERT语句
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf(":col%d", i+1)
	}

	insertSql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		task.table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	var err error
	task.insertStmt, err = task.db.Prepare(insertSql)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %v", err)
	}

	return nil
}

func (task *OracleWriterTask) getTableColumns() ([]string, error) {
	query := `
		SELECT column_name
		FROM user_tab_columns
		WHERE table_name = UPPER(?)
		ORDER BY column_id`

	rows, err := task.db.Query(query, task.table)
	if err != nil {
		return nil, err
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

	return columns, rows.Err()
}

func (task *OracleWriterTask) Prepare() error {
	return nil
}

func (task *OracleWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	compLogger := logger.Component().WithComponent("OracleWriterTask")

	defer func() {
		if task.insertStmt != nil {
			task.insertStmt.Close()
		}
		if task.db != nil {
			task.db.Close()
		}
	}()

	var batch []element.Record
	recordCount := int64(0)

	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == plugin.ErrChannelClosed {
				break
			}
			return fmt.Errorf("failed to get record from reader: %v", err)
		}

		batch = append(batch, record)

		// 当达到批次大小时，执行批量写入
		if len(batch) >= task.batchSize {
			if err := task.writeBatch(batch); err != nil {
				return err
			}
			recordCount += int64(len(batch))
			batch = batch[:0]

			// 输出进度
			compLogger.Debug("Writing progress", zap.Int64("records", recordCount))
		}
	}

	// 写入剩余的记录
	if len(batch) > 0 {
		if err := task.writeBatch(batch); err != nil {
			return err
		}
		recordCount += int64(len(batch))
	}

	compLogger.Info("Write task completed", zap.Int64("totalRecords", recordCount))
	return nil
}

func (task *OracleWriterTask) writeBatch(batch []element.Record) error {
	compLogger := logger.Component().WithComponent("OracleWriterTask")

	// 开始事务
	tx, err := task.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// 在事务中使用prepared statement
	stmt := tx.Stmt(task.insertStmt)
	defer stmt.Close()

	for _, record := range batch {
		// 转换记录数据
		values := make([]interface{}, len(task.columns))
		for i := 0; i < len(task.columns) && i < record.GetColumnNumber(); i++ {
			column := record.GetColumn(i)
			values[i] = task.convertValue(column)
		}

		// 执行插入
		if _, err := stmt.Exec(values...); err != nil {
			compLogger.Error("Failed to execute insert statement",
				zap.Error(err),
				zap.String("table", task.table))
			return fmt.Errorf("failed to execute insert: %v", err)
		}
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

func (task *OracleWriterTask) convertValue(column element.Column) interface{} {
	if column.IsNull() {
		return nil
	}

	switch col := column.(type) {
	case *element.StringColumn:
		return col.GetAsString()
	case *element.LongColumn:
		val, _ := col.GetAsLong()
		return val
	case *element.DoubleColumn:
		val, _ := col.GetAsDouble()
		return val
	case *element.DateColumn:
		val, _ := col.GetAsDate()
		return val
	case *element.BoolColumn:
		// Oracle中布尔值通常用NUMBER(1)表示
		val, _ := col.GetAsBool()
		if val {
			return 1
		}
		return 0
	case *element.BytesColumn:
		val, _ := col.GetAsBytes()
		return val
	default:
		// 默认转换为字符串
		return column.GetAsString()
	}
}

func (task *OracleWriterTask) Post() error {
	return nil
}

func (task *OracleWriterTask) Destroy() error {
	compLogger := logger.Component().WithComponent("OracleWriterTask")
	compLogger.Info("Destroying Oracle writer task")

	if task.insertStmt != nil {
		task.insertStmt.Close()
	}
	if task.db != nil {
		task.db.Close()
	}
	return nil
}