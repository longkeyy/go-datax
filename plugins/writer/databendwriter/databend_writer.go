package databendwriter

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	_ "github.com/datafuselabs/databend-go"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/plugin"
	"go.uber.org/zap"
)

const (
	DefaultBatchSize = 1000
)

// DatabendWriterJob Databend写入作业，遵循Java版本的结构
type DatabendWriterJob struct {
	config            config.Configuration
	username          string
	password          string
	jdbcUrls          []string
	tables            []string
	columns           []string
	preSql            []string
	postSql           []string
	writeMode         string
	onConflictColumns []string
	batchSize         int
}

func NewDatabendWriterJob() *DatabendWriterJob {
	return &DatabendWriterJob{
		writeMode: "insert",
		batchSize: DefaultBatchSize,
	}
}

// Init 初始化作业配置，复制Java版本的参数验证逻辑
func (job *DatabendWriterJob) Init(config config.Configuration) error {
	job.config = config

	// 获取基本认证参数
	job.username = config.GetString("parameter.username")
	job.password = config.GetString("parameter.password")

	if job.username == "" || job.password == "" {
		return fmt.Errorf("username and password are required")
	}

	// 获取连接配置
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

	// 获取列配置
	job.columns = config.GetStringList("parameter.column")
	if len(job.columns) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	// 如果配置为*，需要获取表的实际列名
	if len(job.columns) == 1 && job.columns[0] == "*" {
		actualColumns, err := job.getTableColumns()
		if err != nil {
			return fmt.Errorf("failed to get table columns: %v", err)
		}
		job.columns = actualColumns
	}

	// 获取可选参数
	job.batchSize = config.GetIntWithDefault("parameter.batchSize", DefaultBatchSize)
	job.writeMode = config.GetStringWithDefault("parameter.writeMode", "insert")
	job.preSql = config.GetStringList("parameter.preSql")
	job.postSql = config.GetStringList("parameter.postSql")
	job.onConflictColumns = config.GetStringList("parameter.onConflictColumn")

	// 验证writeMode配置（复制Java版本逻辑）
	return job.validateWriteMode()
}

// validateWriteMode 验证写入模式配置，复制Java版本的验证逻辑
func (job *DatabendWriterJob) validateWriteMode() error {
	if strings.ToLower(job.writeMode) == "replace" {
		if len(job.onConflictColumns) == 0 {
			return fmt.Errorf("replace mode must has onConflictColumn config")
		}
	}
	return nil
}

// getTableColumns 获取表的列信息
func (job *DatabendWriterJob) getTableColumns() ([]string, error) {
	db, err := job.createConnection()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	tableName := job.tables[0]
	query := fmt.Sprintf("DESCRIBE %s", tableName)

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName, columnType, nullable, key, defaultValue, extra string
		err := rows.Scan(&columnName, &columnType, &nullable, &key, &defaultValue, &extra)
		if err != nil {
			return nil, err
		}
		columns = append(columns, columnName)
	}

	return columns, nil
}

// createConnection 创建数据库连接
func (job *DatabendWriterJob) createConnection() (*sql.DB, error) {
	// 解析JDBC URL: jdbc:databend://host:port/database
	jdbcUrl := job.jdbcUrls[0]
	dsn, err := job.parseDSN(jdbcUrl)
	if err != nil {
		return nil, err
	}

	return sql.Open("databend", dsn)
}

// parseDSN 解析JDBC URL为Databend Go driver格式
func (job *DatabendWriterJob) parseDSN(jdbcUrl string) (string, error) {
	// 解析 jdbc:databend://host:port/database 格式
	re := regexp.MustCompile(`^jdbc:databend://([^:/]+):(\d+)/(.+)$`)
	matches := re.FindStringSubmatch(jdbcUrl)
	if len(matches) != 4 {
		return "", fmt.Errorf("invalid JDBC URL format: %s", jdbcUrl)
	}

	host := matches[1]
	port := matches[2]
	database := matches[3]

	// 构建databend-go driver格式的DSN
	return fmt.Sprintf("databend://%s:%s@%s:%s/%s", job.username, job.password, host, port, database), nil
}

// Prepare 准备阶段，执行preSql
func (job *DatabendWriterJob) Prepare() error {
	if len(job.preSql) == 0 {
		return nil
	}

	db, err := job.createConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	for _, sql := range job.preSql {
		logger.App().Info("executing preSql", zap.String("sql", sql))
		if _, err := db.Exec(sql); err != nil {
			return fmt.Errorf("failed to execute preSql [%s]: %v", sql, err)
		}
	}

	return nil
}

// Post 后处理阶段，执行postSql
func (job *DatabendWriterJob) Post() error {
	if len(job.postSql) == 0 {
		return nil
	}

	db, err := job.createConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	for _, sql := range job.postSql {
		logger.App().Info("executing postSql", zap.String("sql", sql))
		if _, err := db.Exec(sql); err != nil {
			return fmt.Errorf("failed to execute postSql [%s]: %v", sql, err)
		}
	}

	return nil
}

// Split 任务分片，与Java版本保持一致
func (job *DatabendWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	splits := make([]config.Configuration, mandatoryNumber)
	for i := 0; i < mandatoryNumber; i++ {
		splits[i] = job.config
	}
	return splits, nil
}

// Destroy 清理资源
func (job *DatabendWriterJob) Destroy() error {
	// 清理操作
	return nil
}

// DatabendWriterTask Databend写入任务
type DatabendWriterTask struct {
	config            config.Configuration
	username          string
	password          string
	jdbcUrl           string
	table             string
	columns           []string
	writeMode         string
	onConflictColumns []string
	batchSize         int
	db                *sql.DB
}

func NewDatabendWriterTask() *DatabendWriterTask {
	return &DatabendWriterTask{
		writeMode: "insert",
		batchSize: DefaultBatchSize,
	}
}

// Init 初始化任务
func (task *DatabendWriterTask) Init(config config.Configuration) error {
	task.config = config

	// 获取配置参数
	task.username = config.GetString("parameter.username")
	task.password = config.GetString("parameter.password")
	task.batchSize = config.GetIntWithDefault("parameter.batchSize", DefaultBatchSize)
	task.writeMode = config.GetStringWithDefault("parameter.writeMode", "insert")
	task.onConflictColumns = config.GetStringList("parameter.onConflictColumn")

	// 获取连接配置
	connections := config.GetListConfiguration("parameter.connection")
	if len(connections) == 0 {
		return fmt.Errorf("connection configuration is required")
	}

	conn := connections[0]
	jdbcUrls := conn.GetStringList("jdbcUrl")
	tables := conn.GetStringList("table")

	if len(jdbcUrls) == 0 || len(tables) == 0 {
		return fmt.Errorf("jdbcUrl and table are required")
	}

	task.jdbcUrl = jdbcUrls[0]
	task.table = tables[0]

	// 获取列配置
	task.columns = config.GetStringList("parameter.column")
	if len(task.columns) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	// 创建数据库连接
	return task.createConnection()
}

// createConnection 创建数据库连接
func (task *DatabendWriterTask) createConnection() error {
	dsn, err := task.parseDSN(task.jdbcUrl)
	if err != nil {
		return err
	}

	task.db, err = sql.Open("databend", dsn)
	return err
}

// parseDSN 解析JDBC URL为Databend Go driver格式
func (task *DatabendWriterTask) parseDSN(jdbcUrl string) (string, error) {
	// 解析 jdbc:databend://host:port/database 格式
	re := regexp.MustCompile(`^jdbc:databend://([^:/]+):(\d+)/(.+)$`)
	matches := re.FindStringSubmatch(jdbcUrl)
	if len(matches) != 4 {
		return "", fmt.Errorf("invalid JDBC URL format: %s", jdbcUrl)
	}

	host := matches[1]
	port := matches[2]
	database := matches[3]

	// 构建databend-go driver格式的DSN
	return fmt.Sprintf("databend://%s:%s@%s:%s/%s", task.username, task.password, host, port, database), nil
}

// Prepare 任务准备
func (task *DatabendWriterTask) Prepare() error {
	return nil
}

// Post 任务后处理
func (task *DatabendWriterTask) Post() error {
	return nil
}

// StartWrite 开始写入数据，实现与Java版本相同的批量写入逻辑
func (task *DatabendWriterTask) StartWrite(receiver plugin.RecordReceiver) error {
	ctx := context.Background()
	batch := make([]element.Record, 0, task.batchSize)

	for {
		record, err := receiver.GetFromReader()
		if err != nil {
			// 处理最后一批数据
			if len(batch) > 0 {
				if writeErr := task.writeBatch(ctx, batch); writeErr != nil {
					return writeErr
				}
			}
			break
		}

		if record == nil {
			continue
		}

		batch = append(batch, record)

		// 当批次达到指定大小时执行写入
		if len(batch) >= task.batchSize {
			if err := task.writeBatch(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0] // 重置batch
		}
	}

	return nil
}

// writeBatch 批量写入数据，复制Java版本的SQL生成逻辑
func (task *DatabendWriterTask) writeBatch(ctx context.Context, records []element.Record) error {
	if len(records) == 0 {
		return nil
	}

	sql := task.buildInsertSQL()
	stmt, err := task.db.PrepareContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	for _, record := range records {
		values := make([]interface{}, len(task.columns))
		for i := range task.columns {
			if i < record.GetColumnNumber() {
				col := record.GetColumn(i)
				values[i] = task.convertColumnValue(col)
			} else {
				values[i] = nil
			}
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			logger.Component().Error("failed to insert record",
				zap.Error(err),
				zap.String("table", task.table))
			return fmt.Errorf("failed to insert record: %v", err)
		}
	}

	logger.Component().Info("batch write completed",
		zap.Int("records", len(records)),
		zap.String("table", task.table))

	return nil
}

// buildInsertSQL 构建插入SQL，复制Java版本的逻辑
func (task *DatabendWriterTask) buildInsertSQL() string {
	var sqlBuilder strings.Builder

	if strings.ToLower(task.writeMode) == "replace" {
		// REPLACE INTO模式
		sqlBuilder.WriteString(fmt.Sprintf("REPLACE INTO %s (", task.table))
		sqlBuilder.WriteString(strings.Join(task.columns, ","))
		sqlBuilder.WriteString(") ")
		sqlBuilder.WriteString(task.buildOnConflictClause())
		sqlBuilder.WriteString(" VALUES (")
	} else {
		// INSERT INTO模式
		sqlBuilder.WriteString(fmt.Sprintf("INSERT INTO %s (", task.table))
		sqlBuilder.WriteString(strings.Join(task.columns, ","))
		sqlBuilder.WriteString(") VALUES (")
	}

	// 添加占位符
	placeholders := make([]string, len(task.columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	sqlBuilder.WriteString(strings.Join(placeholders, ","))
	sqlBuilder.WriteString(")")

	return sqlBuilder.String()
}

// buildOnConflictClause 构建ON CONFLICT子句，复制Java版本的逻辑
func (task *DatabendWriterTask) buildOnConflictClause() string {
	if len(task.onConflictColumns) == 0 {
		return ""
	}
	return fmt.Sprintf("ON (%s)", strings.Join(task.onConflictColumns, ","))
}

// convertColumnValue 类型转换，复制Java版本的类型处理逻辑
func (task *DatabendWriterTask) convertColumnValue(column element.Column) interface{} {
	if column.GetRawData() == nil {
		return nil
	}

	switch column.GetType() {
	case element.TypeBool:
		val, _ := column.GetAsBool()
		return val
	case element.TypeLong:
		val, _ := column.GetAsLong()
		return val
	case element.TypeDouble:
		val, _ := column.GetAsDouble()
		return val
	case element.TypeString:
		return column.GetAsString()
	case element.TypeDate:
		dateValue, err := column.GetAsDate()
		if err == nil {
			return dateValue.Format("2006-01-02 15:04:05")
		}
		return nil
	case element.TypeBytes:
		val, _ := column.GetAsBytes()
		return val
	default:
		// 对于复杂类型（如variant, array），转换为字符串
		return column.GetAsString()
	}
}

// Destroy 销毁任务，清理资源
func (task *DatabendWriterTask) Destroy() error {
	if task.db != nil {
		return task.db.Close()
	}
	return nil
}