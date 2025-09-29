package gaussdbwriter

import (
	"fmt"
	"strings"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/factory"
	"github.com/longkeyy/go-datax/common/database/rdbms/writer"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
)

// GaussDBWriterJob GaussDB写入作业
type GaussDBWriterJob struct {
	config   config.Configuration
	username string
	password string
	jdbcUrls []string
	tables   []string
	columns  []string
	writeMode string // GaussDB只支持insert模式
	batchSize int
	factory  *factory.DataXFactory
}

func NewGaussDBWriterJob() *GaussDBWriterJob {
	return &GaussDBWriterJob{
		factory: factory.GetGlobalFactory(),
	}
}

func (job *GaussDBWriterJob) Init(config config.Configuration) error {
	job.config = config

	// 检查writeMode配置（匹配Java版本警告逻辑）
	writeMode := config.GetString("parameter.writeMode")
	if writeMode != "" {
		return fmt.Errorf("写入模式(writeMode)配置有误. 因为GaussDB不支持配置参数项 writeMode: %s, GaussDB仅使用insert sql 插入数据. 请检查您的配置并作出修改", writeMode)
	}

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

	// 获取批处理大小
	job.batchSize = config.GetIntWithDefault("parameter.batchSize", 1024)
	if job.batchSize < 1 {
		return fmt.Errorf("batchSize must be greater than 0, got: %d", job.batchSize)
	}

	compLogger := logger.Component().WithComponent("GaussDBWriter")
	compLogger.Info("GaussDB Writer initialized",
		zap.String("username", job.username),
		zap.Strings("tables", job.tables),
		zap.Strings("columns", job.columns),
		zap.Int("batchSize", job.batchSize))

	return nil
}

func (job *GaussDBWriterJob) Prepare() error {
	compLogger := logger.Component().WithComponent("GaussDBWriter")
	compLogger.Info("GaussDB Writer job preparation started")

	// 执行RDBMS预处理（包括"*"列的解析）
	if err := writer.DoPretreatment(job.config, writer.GaussDB); err != nil {
		return fmt.Errorf("failed to do pretreatment: %v", err)
	}

	// 重新获取可能被预处理修改的列配置
	job.columns = job.config.GetStringList("parameter.column")

	compLogger.Info("GaussDB Writer job preparation completed", zap.Strings("finalColumns", job.columns))
	return nil
}

func (job *GaussDBWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	compLogger := logger.Component().WithComponent("GaussDBWriter")
	compLogger.Info("Splitting GaussDB Writer job", zap.Int("mandatoryNumber", mandatoryNumber))

	var taskConfigs []config.Configuration

	// 基本的任务分割逻辑（基于表数量）
	totalTables := len(job.tables)
	if totalTables == 0 {
		return nil, fmt.Errorf("no tables to write")
	}

	// 确保至少有一个任务
	taskCount := mandatoryNumber
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
		taskConfig.Set("parameter.batchSize", job.batchSize)

		// 设置连接和表配置
		connectionConfig := config.NewConfiguration()
		connectionConfig.Set("jdbcUrl", job.jdbcUrls[0]) // 简化处理，使用第一个URL
		connectionConfig.Set("table", taskTables)

		taskConfig.Set("parameter.connection", []config.Configuration{connectionConfig})

		taskConfigs = append(taskConfigs, taskConfig)
		start = end
	}

	compLogger.Info("GaussDB Writer job split completed",
		zap.Int("totalTasks", len(taskConfigs)),
		zap.Int("totalTables", totalTables))

	return taskConfigs, nil
}

func (job *GaussDBWriterJob) Post() error {
	compLogger := logger.Component().WithComponent("GaussDBWriter")
	compLogger.Info("GaussDB Writer job post processing completed")
	return nil
}

func (job *GaussDBWriterJob) Destroy() error {
	compLogger := logger.Component().WithComponent("GaussDBWriter")
	compLogger.Info("GaussDB Writer job destroyed")
	return nil
}

// GaussDBWriterTask GaussDB写入任务
type GaussDBWriterTask struct {
	config     config.Configuration
	db         *gorm.DB
	factory    *factory.DataXFactory
	taskID     int
	columns    []string
	tables     []string
	batchSize  int
}

func NewGaussDBWriterTask() *GaussDBWriterTask {
	return &GaussDBWriterTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *GaussDBWriterTask) Init(config config.Configuration) error {
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

	task.tables = connections[0].GetStringList("table")
	task.columns = config.GetStringList("parameter.column")
	task.batchSize = config.GetIntWithDefault("parameter.batchSize", 1024)

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
	taskLogger.Info("GaussDB Writer task initialized",
		zap.String("dsn", dsn),
		zap.Strings("tables", task.tables),
		zap.Strings("columns", task.columns))

	return nil
}

func (task *GaussDBWriterTask) Prepare() error {
	return nil
}

func (task *GaussDBWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	taskLogger := logger.TaskGroupLogger(0).WithTaskId(task.taskID)
	taskLogger.Info("Starting GaussDB Writer task")

	var batch [][]interface{}
	recordCount := 0
	totalRecordCount := 0

	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err.Error() == "no more records" {
				break
			}
			return fmt.Errorf("failed to get record from receiver: %v", err)
		}

		if record == nil {
			break
		}

		// 转换记录为值数组
		values := task.convertRecordToValues(record)
		batch = append(batch, values)
		recordCount++

		// 批量插入
		if recordCount >= task.batchSize {
			if err := task.executeBatch(batch); err != nil {
				return fmt.Errorf("failed to execute batch: %v", err)
			}
			totalRecordCount += recordCount
			batch = batch[:0] // 清空批次
			recordCount = 0

			taskLogger.Debug("Batch written", zap.Int("batchSize", task.batchSize), zap.Int("totalRecords", totalRecordCount))
		}
	}

	// 处理剩余的记录
	if recordCount > 0 {
		if err := task.executeBatch(batch); err != nil {
			return fmt.Errorf("failed to execute final batch: %v", err)
		}
		totalRecordCount += recordCount
	}

	taskLogger.Info("GaussDB Writer task completed", zap.Int("totalRecords", totalRecordCount))
	return nil
}

func (task *GaussDBWriterTask) executeBatch(batch [][]interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	for _, table := range task.tables {
		if err := task.insertBatchToTable(table, batch); err != nil {
			return fmt.Errorf("failed to insert batch to table %s: %v", table, err)
		}
	}

	return nil
}

func (task *GaussDBWriterTask) insertBatchToTable(table string, batch [][]interface{}) error {
	taskLogger := logger.TaskGroupLogger(0).WithTaskId(task.taskID)

	// 构建插入SQL（匹配Java版本的特殊类型处理逻辑）
	columnStr := strings.Join(task.columns, ", ")
	placeholders := make([]string, len(task.columns))
	for i, col := range task.columns {
		// GaussDB特殊类型处理（匹配Java版本的calcValueHolder逻辑）
		placeholder := task.calcValueHolder(col)
		placeholders[i] = placeholder
	}
	placeholderStr := strings.Join(placeholders, ", ")

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, columnStr, placeholderStr)

	taskLogger.Debug("Executing batch insert",
		zap.String("sql", insertSQL),
		zap.Int("batchSize", len(batch)))

	// 开始事务
	tx := task.db.Begin()
	if tx.Error != nil {
		return fmt.Errorf("failed to begin transaction: %v", tx.Error)
	}

	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}
	}()

	// 批量插入
	for _, values := range batch {
		if err := tx.Exec(insertSQL, values...).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute insert: %v", err)
		}
	}

	// 提交事务
	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

// calcValueHolder 处理GaussDB特殊数据类型（匹配Java版本逻辑）
func (task *GaussDBWriterTask) calcValueHolder(columnType string) string {
	columnTypeLower := strings.ToLower(columnType)

	// 匹配Java版本的特殊类型处理
	switch columnTypeLower {
	case "serial":
		return "?::int"
	case "bigserial":
		return "?::int8"
	case "bit":
		return "?::bit varying"
	default:
		return "?::" + columnTypeLower
	}
}

func (task *GaussDBWriterTask) convertRecordToValues(record element.Record) []interface{} {
	columnCount := record.GetColumnNumber()
	values := make([]interface{}, columnCount)

	for i := 0; i < columnCount; i++ {
		column := record.GetColumn(i)
		if column == nil {
			values[i] = nil
			continue
		}

		switch column.GetType() {
		case element.TypeString:
			values[i] = column.GetAsString()
		case element.TypeLong:
			if v, err := column.GetAsLong(); err == nil {
				values[i] = v
			} else {
				values[i] = nil
			}
		case element.TypeDouble:
			if v, err := column.GetAsDouble(); err == nil {
				values[i] = v
			} else {
				values[i] = nil
			}
		case element.TypeDate:
			if v, err := column.GetAsDate(); err == nil {
				values[i] = v
			} else {
				values[i] = nil
			}
		case element.TypeBool:
			if v, err := column.GetAsBool(); err == nil {
				values[i] = v
			} else {
				values[i] = nil
			}
		case element.TypeBytes:
			if v, err := column.GetAsBytes(); err == nil {
				values[i] = v
			} else {
				values[i] = nil
			}
		default:
			values[i] = column.GetAsString()
		}
	}

	return values
}

func (task *GaussDBWriterTask) Post() error {
	return nil
}

func (task *GaussDBWriterTask) Destroy() error {
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