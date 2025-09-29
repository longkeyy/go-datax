package gaussdbreader

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
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
)

const (
	DefaultFetchSize = 1000 // 匹配Java版本的DEFAULT_FETCH_SIZE
)

// GaussDBReaderJob GaussDB读取作业
type GaussDBReaderJob struct {
	config   config.Configuration
	username string
	password string
	jdbcUrls []string
	tables   []string
	columns  []string
	where    string
	splitPk  string
	querySql string // 支持自定义查询SQL
	factory  *factory.DataXFactory
}

func NewGaussDBReaderJob() *GaussDBReaderJob {
	return &GaussDBReaderJob{
		factory: factory.GetGlobalFactory(),
	}
}

func (job *GaussDBReaderJob) Init(config config.Configuration) error {
	job.config = config

	// 获取数据库连接参数
	job.username = config.GetString("parameter.username")
	job.password = config.GetString("parameter.password")

	// 获取连接URL列表
	connections := config.GetListConfiguration("parameter.connection")
	if len(connections) == 0 {
		return fmt.Errorf("connection configuration is required")
	}

	// 获取JDBC URL和表配置
	for _, conn := range connections {
		jdbcUrl := conn.GetString("jdbcUrl")
		if jdbcUrl == "" {
			return fmt.Errorf("jdbcUrl is required")
		}
		job.jdbcUrls = append(job.jdbcUrls, jdbcUrl)

		tables := conn.GetStringList("table")
		if len(tables) == 0 {
			return fmt.Errorf("table configuration is required")
		}
		job.tables = append(job.tables, tables...)
	}

	// 获取列配置
	job.columns = config.GetStringList("parameter.column")
	if len(job.columns) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	// 获取可选配置
	job.where = config.GetString("parameter.where")
	job.splitPk = config.GetString("parameter.splitPk")
	job.querySql = config.GetString("parameter.querySql")

	// 验证fetchSize配置（匹配Java版本逻辑）
	fetchSize := config.GetIntWithDefault("parameter.fetchSize", DefaultFetchSize)
	if fetchSize < 1 {
		return fmt.Errorf("您配置的fetchSize有误，根据DataX的设计，fetchSize : [%d] 设置值不能小于 1", fetchSize)
	}
	job.config.Set("parameter.fetchSize", fetchSize)

	compLogger := logger.Component().WithComponent("GaussDBReader")
	compLogger.Info("GaussDB Reader initialized",
		zap.String("username", job.username),
		zap.Strings("tables", job.tables),
		zap.Strings("columns", job.columns),
		zap.Int("fetchSize", fetchSize))

	return nil
}

func (job *GaussDBReaderJob) Prepare() error {
	compLogger := logger.Component().WithComponent("GaussDBReader")
	compLogger.Info("GaussDB Reader job preparation started")

	// 这里可以添加准备阶段的逻辑，如验证表结构等
	return nil
}

func (job *GaussDBReaderJob) Split(adviceNumber int) ([]config.Configuration, error) {
	compLogger := logger.Component().WithComponent("GaussDBReader")
	compLogger.Info("Splitting GaussDB Reader job", zap.Int("adviceNumber", adviceNumber))

	var taskConfigs []config.Configuration

	// 基本的任务分割逻辑（基于表数量）
	totalTables := len(job.tables)
	if totalTables == 0 {
		return nil, fmt.Errorf("no tables to read")
	}

	// 确保至少有一个任务
	taskCount := adviceNumber
	if taskCount > totalTables {
		taskCount = totalTables
	}
	if taskCount < 1 {
		taskCount = 1
	}

	// 将表分配给不同的任务
	tablesPerTask := totalTables / taskCount
	remainder := totalTables % taskCount

	start := 0
	for i := 0; i < taskCount; i++ {
		taskTableCount := tablesPerTask
		if i < remainder {
			taskTableCount++
		}

		if taskTableCount == 0 {
			break
		}

		end := start + taskTableCount
		taskTables := job.tables[start:end]

		// 创建任务配置
		taskConfig := config.NewConfiguration()
		taskConfig.Set("parameter.username", job.username)
		taskConfig.Set("parameter.password", job.password)
		taskConfig.Set("parameter.column", job.columns)
		taskConfig.Set("parameter.fetchSize", job.config.GetIntWithDefault("parameter.fetchSize", DefaultFetchSize))

		if job.where != "" {
			taskConfig.Set("parameter.where", job.where)
		}
		if job.splitPk != "" {
			taskConfig.Set("parameter.splitPk", job.splitPk)
		}
		if job.querySql != "" {
			taskConfig.Set("parameter.querySql", job.querySql)
		}

		// 设置连接和表配置
		connectionConfig := config.NewConfiguration()
		connectionConfig.Set("jdbcUrl", job.jdbcUrls[0]) // 简化处理，使用第一个URL
		connectionConfig.Set("table", taskTables)

		taskConfig.Set("parameter.connection", []config.Configuration{connectionConfig})

		taskConfigs = append(taskConfigs, taskConfig)
		start = end
	}

	compLogger.Info("GaussDB Reader job split completed",
		zap.Int("totalTasks", len(taskConfigs)),
		zap.Int("totalTables", totalTables))

	return taskConfigs, nil
}

func (job *GaussDBReaderJob) Post() error {
	compLogger := logger.Component().WithComponent("GaussDBReader")
	compLogger.Info("GaussDB Reader job post processing completed")
	return nil
}

func (job *GaussDBReaderJob) Destroy() error {
	compLogger := logger.Component().WithComponent("GaussDBReader")
	compLogger.Info("GaussDB Reader job destroyed")
	return nil
}

// GaussDBReaderTask GaussDB读取任务
type GaussDBReaderTask struct {
	config     config.Configuration
	db         *gorm.DB
	factory    *factory.DataXFactory
	taskID     int
}

func NewGaussDBReaderTask() *GaussDBReaderTask {
	return &GaussDBReaderTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *GaussDBReaderTask) Init(config config.Configuration) error {
	task.config = config

	username := config.GetString("parameter.username")
	password := config.GetString("parameter.password")

	connections := config.GetListConfiguration("parameter.connection")
	if len(connections) == 0 {
		return fmt.Errorf("no connection configuration found")
	}

	jdbcUrl := connections[0].GetString("jdbcUrl")
	if jdbcUrl == "" {
		return fmt.Errorf("jdbcUrl is required")
	}

	// 转换JDBC URL为PostgreSQL DSN（GaussDB使用PostgreSQL协议）
	dsn, err := convertJDBCToPostgresDSN(jdbcUrl, username, password)
	if err != nil {
		return fmt.Errorf("failed to convert JDBC URL: %v", err)
	}

	// 创建数据库连接
	task.db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: gormLogger.Default.LogMode(gormLogger.Silent),
	})
	if err != nil {
		return fmt.Errorf("failed to connect to GaussDB: %v", err)
	}

	// 配置连接池
	sqlDB, err := task.db.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying sql.DB: %v", err)
	}

	sqlDB.SetMaxOpenConns(10)
	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetConnMaxLifetime(time.Hour)

	taskLogger := logger.TaskGroupLogger(0).WithTaskId(task.taskID)
	taskLogger.Info("GaussDB Reader task initialized", zap.String("dsn", dsn))

	return nil
}

func (task *GaussDBReaderTask) Prepare() error {
	return nil
}

func (task *GaussDBReaderTask) StartRead(recordSender plugin.RecordSender) error {
	taskLogger := logger.TaskGroupLogger(0).WithTaskId(task.taskID)
	taskLogger.Info("Starting GaussDB Reader task")

	connections := task.config.GetListConfiguration("parameter.connection")
	tables := connections[0].GetStringList("table")
	columns := task.config.GetStringList("parameter.column")
	where := task.config.GetString("parameter.where")
	querySql := task.config.GetString("parameter.querySql")
	fetchSize := task.config.GetIntWithDefault("parameter.fetchSize", DefaultFetchSize)

	for _, table := range tables {
		var query string

		if querySql != "" {
			// 使用自定义查询SQL
			query = querySql
		} else {
			// 构建标准查询
			columnStr := strings.Join(columns, ", ")
			query = fmt.Sprintf("SELECT %s FROM %s", columnStr, table)

			if where != "" {
				query += " WHERE " + where
			}
		}

		if err := task.executeQuery(query, fetchSize, recordSender, taskLogger); err != nil {
			return fmt.Errorf("failed to execute query for table %s: %v", table, err)
		}
	}

	taskLogger.Info("GaussDB Reader task completed")
	return nil
}

func (task *GaussDBReaderTask) executeQuery(query string, fetchSize int, recordSender plugin.RecordSender, taskLogger logger.TaskLogger) error {
	taskLogger.Info("Executing query", zap.String("query", query), zap.Int("fetchSize", fetchSize))

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

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("failed to get column types: %v", err)
	}

	recordCount := 0
	for rows.Next() {
		record := element.NewRecord()

		// 创建接收数据的slice
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 扫描行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		// 转换数据并添加到记录中
		for i, value := range values {
			column := task.convertValue(value, columnTypes[i])
			record.AddColumn(column)
		}

		// 发送记录
		if err := recordSender.SendRecord(record); err != nil {
			return fmt.Errorf("failed to send record: %v", err)
		}

		recordCount++
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %v", err)
	}

	taskLogger.Info("Query execution completed", zap.Int("recordCount", recordCount))
	return nil
}

func (task *GaussDBReaderTask) convertValue(value interface{}, colType *sql.ColumnType) element.Column {
	if value == nil {
		return element.NewStringColumn("")
	}

	switch v := value.(type) {
	case []byte:
		return element.NewStringColumn(string(v))
	case string:
		return element.NewStringColumn(v)
	case int64:
		return element.NewLongColumn(v)
	case int32:
		return element.NewLongColumn(int64(v))
	case float64:
		return element.NewDoubleColumn(v)
	case float32:
		return element.NewDoubleColumn(float64(v))
	case bool:
		return element.NewBoolColumn(v)
	case time.Time:
		return element.NewDateColumn(v)
	default:
		// 对于其他类型，转换为字符串
		return element.NewStringColumn(fmt.Sprintf("%v", v))
	}
}

func (task *GaussDBReaderTask) Post() error {
	return nil
}

func (task *GaussDBReaderTask) Destroy() error {
	if task.db != nil {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}
	return nil
}

// convertJDBCToPostgresDSN 将JDBC URL转换为PostgreSQL DSN格式
func convertJDBCToPostgresDSN(jdbcUrl, username, password string) (string, error) {
	// 移除jdbc:前缀
	if strings.HasPrefix(jdbcUrl, "jdbc:") {
		jdbcUrl = strings.TrimPrefix(jdbcUrl, "jdbc:")
	}

	// 对于GaussDB，通常使用postgresql://或opengauss://协议
	var dsn string
	if strings.HasPrefix(jdbcUrl, "postgresql://") {
		// 已经是标准PostgreSQL URL格式
		dsn = jdbcUrl
	} else if strings.HasPrefix(jdbcUrl, "opengauss://") {
		// OpenGauss URL，转换为PostgreSQL格式
		dsn = strings.Replace(jdbcUrl, "opengauss://", "postgresql://", 1)
	} else if strings.HasPrefix(jdbcUrl, "gaussdb://") {
		// GaussDB URL，转换为PostgreSQL格式
		dsn = strings.Replace(jdbcUrl, "gaussdb://", "postgresql://", 1)
	} else {
		return "", fmt.Errorf("unsupported JDBC URL format: %s", jdbcUrl)
	}

	// 如果URL中没有用户名密码，添加它们
	if !strings.Contains(dsn, "@") && username != "" {
		// 找到://后的位置
		protocolEnd := strings.Index(dsn, "://") + 3
		if protocolEnd > 3 {
			userInfo := username
			if password != "" {
				userInfo += ":" + password
			}
			dsn = dsn[:protocolEnd] + userInfo + "@" + dsn[protocolEnd:]
		}
	}

	return dsn, nil
}