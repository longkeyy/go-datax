package oceanbasewriter

import (
	"fmt"
	"strings"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/factory"
	coreplugin "github.com/longkeyy/go-datax/core/registry"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
)

const (
	DefaultBatchSize          = 1024
	OBSplitString             = "||_dsc_ob10_dsc_||"
	OBSplitStringPattern      = "\\|\\|_dsc_ob10_dsc_\\|\\|"
	OBCompatibleModeMySQL     = "MYSQL"
	OBCompatibleModeOracle    = "ORACLE"
)

// OceanBaseWriterJob OceanBase写入作业
type OceanBaseWriterJob struct {
	config         config.Configuration
	username       string
	password       string
	jdbcUrls       []string
	tables         []string
	columns        []string
	preSql         []string
	postSql        []string
	writeMode      string
	batchSize      int
	sessionConf    []string
	compatibleMode string // 兼容模式：MYSQL或ORACLE
	factory        *factory.DataXFactory
}

func NewOceanBaseWriterJob() *OceanBaseWriterJob {
	return &OceanBaseWriterJob{
		factory:        factory.GetGlobalFactory(),
		compatibleMode: OBCompatibleModeMySQL, // 默认MySQL模式
	}
}

func (job *OceanBaseWriterJob) Init(config config.Configuration) error {
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

	// 处理OceanBase特有的JDBC URL格式
	if err := job.processOBJdbcUrls(); err != nil {
		return err
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
			logger.Component().WithComponent("OceanBaseWriter").Warn("Failed to get table columns, will use * as is",
				zap.Error(err))
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

	// 检测兼容模式并转义关键字
	if err := job.detectCompatibleMode(); err != nil {
		logger.Component().WithComponent("OceanBaseWriter").Warn("Failed to detect compatible mode, using default MySQL mode",
			zap.Error(err))
	}

	// 根据兼容模式转义关键字（OceanBase特有）
	job.escapeKeywords()

	logger.Component().WithComponent("OceanBaseWriter").Info("OceanBase Writer initialized",
		zap.Any("tables", job.tables),
		zap.Any("columns", job.columns),
		zap.String("writeMode", job.writeMode),
		zap.Int("batchSize", job.batchSize),
		zap.String("compatibleMode", job.compatibleMode))
	return nil
}

// processOBJdbcUrls 处理OceanBase特有的JDBC URL格式
func (job *OceanBaseWriterJob) processOBJdbcUrls() error {
	processedUrls := make([]string, 0, len(job.jdbcUrls))

	for _, jdbcUrl := range job.jdbcUrls {
		// 检查是否是OceanBase 1.0格式的URL
		if strings.HasPrefix(jdbcUrl, OBSplitString) {
			parts := strings.Split(jdbcUrl, OBSplitString)
			if len(parts) != 3 {
				return fmt.Errorf("invalid OceanBase JDBC URL format: %s", jdbcUrl)
			}

			// 提取tenant信息并合并到username
			tenant := strings.TrimSpace(parts[1])
			actualJdbcUrl := parts[2]

			// 如果tenant包含冒号，需要提取tenant名
			if colonIdx := strings.Index(tenant, ":"); colonIdx != -1 {
				tenant = tenant[:colonIdx]
			}

			job.username = tenant + ":" + job.username
			processedUrls = append(processedUrls, actualJdbcUrl)

			logger.Component().WithComponent("OceanBaseWriter").Info("Processed OceanBase 1.0 JDBC URL",
				zap.String("originalUrl", jdbcUrl),
				zap.String("processedUrl", actualJdbcUrl),
				zap.String("tenant", tenant))
		} else {
			processedUrls = append(processedUrls, jdbcUrl)
		}
	}

	job.jdbcUrls = processedUrls
	return nil
}

// detectCompatibleMode 检测OceanBase兼容模式
func (job *OceanBaseWriterJob) detectCompatibleMode() error {
	db, err := job.connect()
	if err != nil {
		return err
	}
	defer func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	// 查询兼容模式
	var compatibleMode string
	query := "SHOW VARIABLES LIKE 'ob_compatibility_mode'"
	err = db.Raw(query).Row().Scan(&compatibleMode)
	if err != nil {
		// 如果查询失败，保持默认的MySQL模式
		return err
	}

	if strings.Contains(strings.ToUpper(compatibleMode), "ORACLE") {
		job.compatibleMode = OBCompatibleModeOracle
	} else {
		job.compatibleMode = OBCompatibleModeMySQL
	}

	logger.Component().WithComponent("OceanBaseWriter").Info("Detected OceanBase compatible mode",
		zap.String("compatibleMode", job.compatibleMode))
	return nil
}

// escapeKeywords 转义OceanBase关键字
func (job *OceanBaseWriterJob) escapeKeywords() {
	escapeChar := "`"
	if job.compatibleMode == OBCompatibleModeOracle {
		escapeChar = "\""
	}

	// 转义列名
	for i, column := range job.columns {
		if column != "*" && !strings.HasPrefix(column, escapeChar) {
			job.columns[i] = escapeChar + column + escapeChar
		}
	}

	// 转义表名
	for i, table := range job.tables {
		if !strings.HasPrefix(table, escapeChar) {
			job.tables[i] = escapeChar + table + escapeChar
		}
	}
}

func (job *OceanBaseWriterJob) Prepare() error {
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

		for _, table := range job.tables {
			for _, sqlTemplate := range job.preSql {
				// 替换表名占位符
				sql := strings.ReplaceAll(sqlTemplate, "@table", table)
				logger.Component().WithComponent("OceanBaseWriter").Info("Executing pre SQL",
					zap.String("sql", sql))
				if err := db.Exec(sql).Error; err != nil {
					return fmt.Errorf("failed to execute pre SQL: %v", err)
				}
			}
		}
	}
	return nil
}

func (job *OceanBaseWriterJob) getTableColumns() ([]string, error) {
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

	rows, err := db.Raw(query, strings.Trim(job.tables[0], "`\"")).Rows()
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

	logger.Component().WithComponent("OceanBaseWriter").Info("Retrieved table columns",
		zap.String("table", job.tables[0]),
		zap.Any("columns", columns))
	return columns, nil
}

func (job *OceanBaseWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	taskConfigs := make([]config.Configuration, 0)

	// OceanBase Writer通常不需要分片，每个task写入相同的表
	for i := 0; i < mandatoryNumber; i++ {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	logger.Component().WithComponent("OceanBaseWriter").Info("Split into OceanBase writer tasks",
		zap.Int("taskCount", len(taskConfigs)))
	return taskConfigs, nil
}

func (job *OceanBaseWriterJob) connect() (*gorm.DB, error) {
	// 构建连接字符串
	jdbcUrl := job.jdbcUrls[0]

	// 转换JDBC URL为MySQL连接字符串（OceanBase使用MySQL协议）
	dsn, err := job.convertJdbcUrl(jdbcUrl)
	if err != nil {
		return nil, err
	}

	// 连接数据库
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: gormLogger.Default.LogMode(gormLogger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	// 应用session配置
	if len(job.sessionConf) > 0 {
		for _, sessionSQL := range job.sessionConf {
			if err := db.Exec(sessionSQL).Error; err != nil {
				logger.Component().WithComponent("OceanBaseWriter").Warn("Failed to execute session SQL",
					zap.String("sql", sessionSQL),
					zap.Error(err))
			}
		}
	}

	return db, nil
}

func (job *OceanBaseWriterJob) convertJdbcUrl(jdbcUrl string) (string, error) {
	// 处理MySQL协议的JDBC URL
	var url string
	if strings.HasPrefix(jdbcUrl, "jdbc:mysql://") {
		url = strings.TrimPrefix(jdbcUrl, "jdbc:mysql://")
	} else if strings.HasPrefix(jdbcUrl, "jdbc:oceanbase://") {
		// OceanBase也可能使用oceanbase协议，转换为mysql协议
		url = strings.TrimPrefix(jdbcUrl, "jdbc:oceanbase://")
	} else {
		return "", fmt.Errorf("unsupported JDBC URL protocol: %s", jdbcUrl)
	}

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

	// 构建MySQL DSN字符串，按照OceanBase优化添加参数
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&loc=Local&yearIsDateType=false&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&socketTimeout=1800s&connectTimeout=60s",
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

func (job *OceanBaseWriterJob) Post() error {
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

		for _, table := range job.tables {
			for _, sqlTemplate := range job.postSql {
				// 替换表名占位符
				sql := strings.ReplaceAll(sqlTemplate, "@table", table)
				logger.Component().WithComponent("OceanBaseWriter").Info("Executing post SQL",
					zap.String("sql", sql))
				if err := db.Exec(sql).Error; err != nil {
					return fmt.Errorf("failed to execute post SQL: %v", err)
				}
			}
		}
	}
	return nil
}

func (job *OceanBaseWriterJob) Destroy() error {
	return nil
}

// OceanBaseWriterTask OceanBase写入任务
type OceanBaseWriterTask struct {
	config    config.Configuration
	writerJob *OceanBaseWriterJob
	db        *gorm.DB
	records   []element.Record
	factory   *factory.DataXFactory
}

func NewOceanBaseWriterTask() *OceanBaseWriterTask {
	return &OceanBaseWriterTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *OceanBaseWriterTask) Init(config config.Configuration) error {
	task.config = config

	// 创建WriterJob来重用连接逻辑
	task.writerJob = NewOceanBaseWriterJob()
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

func (task *OceanBaseWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
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

	logger.Component().WithComponent("OceanBaseWriter").Info("Writing completed",
		zap.Int("totalRecords", recordCount))
	return nil
}

func (task *OceanBaseWriterTask) flushRecords() error {
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

func (task *OceanBaseWriterTask) insertRecords() error {
	table := task.writerJob.tables[0]
	columns := task.writerJob.columns

	// 构建INSERT语句
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

func (task *OceanBaseWriterTask) replaceRecords() error {
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

func (task *OceanBaseWriterTask) updateRecords() error {
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
		// 去掉引号来使用VALUES函数
		columnName := strings.Trim(columns[i], "`\"")
		updateClauses = append(updateClauses, fmt.Sprintf("%s=VALUES(%s)", columns[i], columnName))
	}

	// 构建批量INSERT ... ON DUPLICATE KEY UPDATE语句
	valueClause := "(" + strings.Join(placeholders, ", ") + ")"
	valueClauses := make([]string, len(task.records))
	for i := range valueClauses {
		valueClauses[i] = valueClause
	}

	batchSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
		table,
		strings.Join(columns, ", "),
		strings.Join(valueClauses, ", "),
		strings.Join(updateClauses, ", "))

	// 执行批量更新插入
	if err := task.db.Exec(batchSQL, values...).Error; err != nil {
		return fmt.Errorf("failed to update records: %v", err)
	}

	return nil
}

func (task *OceanBaseWriterTask) convertColumnValue(column element.Column) interface{} {
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

func (task *OceanBaseWriterTask) Prepare() error {
	return nil
}

func (task *OceanBaseWriterTask) Post() error {
	return nil
}

func (task *OceanBaseWriterTask) Destroy() error {
	if task.db != nil {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}
	return nil
}