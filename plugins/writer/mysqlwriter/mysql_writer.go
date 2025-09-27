package mysqlwriter

import (
	"fmt"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"log"
	"strings"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	DefaultBatchSize = 1024
)

// MySQLWriterJob MySQL写入作业
type MySQLWriterJob struct {
	config       *config.Configuration
	username     string
	password     string
	jdbcUrls     []string
	tables       []string
	columns      []string
	preSql       []string
	postSql      []string
	writeMode    string
	batchSize    int
	sessionConf  []string
}

func NewMySQLWriterJob() *MySQLWriterJob {
	return &MySQLWriterJob{}
}

func (job *MySQLWriterJob) Init(config *config.Configuration) error {
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

	// 获取session配置
	job.sessionConf = config.GetStringList("parameter.session")

	log.Printf("MySQL Writer initialized: tables=%v, columns=%v, writeMode=%s, batchSize=%d",
		job.tables, job.columns, job.writeMode, job.batchSize)
	return nil
}

func (job *MySQLWriterJob) Prepare() error {
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

func (job *MySQLWriterJob) getTableColumns() ([]string, error) {
	db, err := job.connect()
	if err != nil {
		return nil, err
	}
	defer func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	// 查询表的列信息
	query := `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = DATABASE()
		AND table_name = ?
		ORDER BY ordinal_position`

	rows, err := db.Raw(query, job.tables[0]).Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to query table columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan column name: %v", err)
		}
		columns = append(columns, columnName)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %s", job.tables[0])
	}

	log.Printf("Retrieved table columns for %s: %v", job.tables[0], columns)
	return columns, nil
}

func (job *MySQLWriterJob) Split(adviceNumber int) ([]*config.Configuration, error) {
	taskConfigs := make([]*config.Configuration, 0)

	// MySQL Writer通常不需要分片，每个task写入相同的表
	for i := 0; i < adviceNumber; i++ {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	log.Printf("Split into %d MySQL writer tasks", len(taskConfigs))
	return taskConfigs, nil
}

func (job *MySQLWriterJob) connect() (*gorm.DB, error) {
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

	// 应用session配置
	if len(job.sessionConf) > 0 {
		for _, sessionSQL := range job.sessionConf {
			if err := db.Exec(sessionSQL).Error; err != nil {
				log.Printf("Warning: failed to execute session SQL: %s, error: %v", sessionSQL, err)
			}
		}
	}

	return db, nil
}

func (job *MySQLWriterJob) convertJdbcUrl(jdbcUrl string) (string, error) {
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

	// 构建MySQL DSN字符串，按照DataX规范自动添加参数
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&loc=Local&yearIsDateType=false&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true",
		job.username,
		job.password,
		hostPort,
		database)

	// 添加用户配置的额外参数
	if params != "" {
		dsn += "&" + params
	}

	return dsn, nil
}

func (job *MySQLWriterJob) Post() error {
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

func (job *MySQLWriterJob) Destroy() error {
	return nil
}

// MySQLWriterTask MySQL写入任务
type MySQLWriterTask struct {
	config    *config.Configuration
	writerJob *MySQLWriterJob
	db        *gorm.DB
	records   []element.Record
}

func NewMySQLWriterTask() *MySQLWriterTask {
	return &MySQLWriterTask{}
}

func (task *MySQLWriterTask) Init(config *config.Configuration) error {
	task.config = config

	// 创建WriterJob来重用连接逻辑
	task.writerJob = NewMySQLWriterJob()
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

func (task *MySQLWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	defer func() {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	recordCount := 0
	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == plugin.ErrChannelClosed {
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

func (task *MySQLWriterTask) flushRecords() error {
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
		return fmt.Errorf("unsupported write mode: %s, supported modes: insert, replace, update", task.writerJob.writeMode)
	}
}

func (task *MySQLWriterTask) insertRecords() error {
	table := task.writerJob.tables[0]
	columns := task.writerJob.columns

	// 构建INSERT语句
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	// sql := fmt.Sprintf("INSERT INTO `%s` (`%s`) VALUES (%s)",
	//	table,
	//	strings.Join(columns, "`, `"),
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

	batchSQL := fmt.Sprintf("INSERT INTO `%s` (`%s`) VALUES %s",
		table,
		strings.Join(columns, "`, `"),
		strings.Join(valueClauses, ", "))

	// 执行批量插入
	if err := task.db.Exec(batchSQL, values...).Error; err != nil {
		return fmt.Errorf("failed to insert records: %v", err)
	}

	return nil
}

func (task *MySQLWriterTask) replaceRecords() error {
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

	batchSQL := fmt.Sprintf("REPLACE INTO `%s` (`%s`) VALUES %s",
		table,
		strings.Join(columns, "`, `"),
		strings.Join(valueClauses, ", "))

	// 执行批量替换
	if err := task.db.Exec(batchSQL, values...).Error; err != nil {
		return fmt.Errorf("failed to replace records: %v", err)
	}

	return nil
}

func (task *MySQLWriterTask) updateRecords() error {
	table := task.writerJob.tables[0]
	columns := task.writerJob.columns

	// 构建INSERT ... ON DUPLICATE KEY UPDATE语句
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

	// 构建UPDATE子句 - 除第一列（通常是主键）外的所有列
	updateClauses := make([]string, 0, len(columns)-1)
	for i := 1; i < len(columns); i++ {
		updateClauses = append(updateClauses, fmt.Sprintf("`%s`=VALUES(`%s`)", columns[i], columns[i]))
	}

	// 构建批量INSERT ... ON DUPLICATE KEY UPDATE语句
	valueClause := "(" + strings.Join(placeholders, ", ") + ")"
	valueClauses := make([]string, len(task.records))
	for i := range valueClauses {
		valueClauses[i] = valueClause
	}

	batchSQL := fmt.Sprintf("INSERT INTO `%s` (`%s`) VALUES %s ON DUPLICATE KEY UPDATE %s",
		table,
		strings.Join(columns, "`, `"),
		strings.Join(valueClauses, ", "),
		strings.Join(updateClauses, ", "))

	// 执行批量更新插入
	if err := task.db.Exec(batchSQL, values...).Error; err != nil {
		return fmt.Errorf("failed to update records: %v", err)
	}

	return nil
}

func (task *MySQLWriterTask) convertColumnValue(column element.Column) interface{} {
	if column == nil {
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
		val, _ := col.GetAsBool()
		return val
	default:
		// 其他类型作为字符串处理
		return column.GetAsString()
	}
}

func (task *MySQLWriterTask) Prepare() error {
	return nil
}

func (task *MySQLWriterTask) Post() error {
	return nil
}

func (task *MySQLWriterTask) Destroy() error {
	if task.db != nil {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}
	return nil
}