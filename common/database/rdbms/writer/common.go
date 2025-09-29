package writer

import (
	"context"
	"fmt"
	"strings"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/plugin"
	coreplugin "github.com/longkeyy/go-datax/core/registry"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	DefaultBatchSize = 1024
)

// CommonRdbmsWriterJob provides unified database write operations across
// different RDBMS types (PostgreSQL, MySQL, SQLite) with configuration preprocessing.
type CommonRdbmsWriterJob struct {
	dataBaseType DatabaseType
	config       config.Configuration
}

// NewCommonRdbmsWriterJob creates a new database writer job for the specified database type.
func NewCommonRdbmsWriterJob(dbType DatabaseType) *CommonRdbmsWriterJob {
	return &CommonRdbmsWriterJob{
		dataBaseType: dbType,
	}
}

// Init performs configuration validation and preprocessing.
// This is where table metadata is resolved and wildcard columns are expanded.
func (job *CommonRdbmsWriterJob) Init(originalConfig config.Configuration) error {
	job.config = originalConfig

	// Critical step: resolve table schemas and expand wildcard columns
	if err := DoPretreatment(originalConfig, job.dataBaseType); err != nil {
		return fmt.Errorf("pretreatment failed: %v", err)
	}

	logger.Component().WithComponent("CommonRdbmsWriterJob").Info("Initialized",
		zap.String("databaseType", job.dataBaseType.String()))
	return nil
}

// Prepare executes pre-processing SQL statements for table setup or data preparation.
func (job *CommonRdbmsWriterJob) Prepare() error {
	preSqls := job.config.GetStringList("parameter.preSql")
	if len(preSqls) == 0 {
		return nil
	}

	db, err := job.createConnection()
	if err != nil {
		return fmt.Errorf("failed to connect for prepare: %v", err)
	}
	defer func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	compLogger := logger.Component().WithComponent("CommonRdbmsWriterJob")
	for _, sql := range preSqls {
		compLogger.Info("Executing preSql", zap.String("sql", sql))
		if err := db.Exec(sql).Error; err != nil {
			return fmt.Errorf("failed to execute preSql [%s]: %v", sql, err)
		}
	}

	return nil
}

// Split creates task configurations for parallel execution.
// Marks configurations as preprocessed to avoid redundant schema resolution.
func (job *CommonRdbmsWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	taskConfigs := make([]config.Configuration, mandatoryNumber)

	for i := 0; i < mandatoryNumber; i++ {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		// Optimization: prevent redundant preprocessing in task instances
		taskConfig.Set("_rdbmsPreprocessed", true)
		taskConfigs[i] = taskConfig
	}

	logger.Component().WithComponent("CommonRdbmsWriterJob").Info("Split writer tasks",
		zap.Int("taskCount", mandatoryNumber),
		zap.String("databaseType", job.dataBaseType.String()))
	return taskConfigs, nil
}

// Post executes post-processing SQL statements for cleanup or finalization.
func (job *CommonRdbmsWriterJob) Post() error {
	postSqls := job.config.GetStringList("parameter.postSql")
	if len(postSqls) == 0 {
		return nil
	}

	db, err := job.createConnection()
	if err != nil {
		return fmt.Errorf("failed to connect for post: %v", err)
	}
	defer func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	compLogger := logger.Component().WithComponent("CommonRdbmsWriterJob")
	for _, sql := range postSqls {
		compLogger.Info("Executing postSql", zap.String("sql", sql))
		if err := db.Exec(sql).Error; err != nil {
			return fmt.Errorf("failed to execute postSql [%s]: %v", sql, err)
		}
	}

	return nil
}

// Destroy performs cleanup operations for the job.
func (job *CommonRdbmsWriterJob) Destroy() error {
	return nil
}

// createConnection establishes database connection using configuration parameters.
func (job *CommonRdbmsWriterJob) createConnection() (*gorm.DB, error) {
	username := job.config.GetString("parameter.username")
	password := job.config.GetString("parameter.password")

	connections := job.config.GetListConfiguration("parameter.connection")
	if len(connections) == 0 {
		return nil, fmt.Errorf("no connection configuration found")
	}

	conn := connections[0]
	jdbcUrl := conn.GetString("jdbcUrl")

	return createConnectionFromJDBC(job.dataBaseType, jdbcUrl, username, password)
}

// CommonRdbmsWriterTask handles the actual data writing operations
// with batching, type conversion, and database-specific optimizations.
type CommonRdbmsWriterTask struct {
	dataBaseType DatabaseType
	config       config.Configuration
	writerJob    *CommonRdbmsWriterJob
	db           *gorm.DB
	batchSize    int
	table        string
	columns      []string
	columnNumber int
}

// NewCommonRdbmsWriterTask creates a new task instance for data writing operations.
func NewCommonRdbmsWriterTask(dbType DatabaseType) *CommonRdbmsWriterTask {
	return &CommonRdbmsWriterTask{
		dataBaseType: dbType,
	}
}

// Init prepares the task with database connection and validates configuration.
func (task *CommonRdbmsWriterTask) Init(config config.Configuration) error {
	task.config = config
	task.batchSize = config.GetIntWithDefault("parameter.batchSize", DefaultBatchSize)

	// Skip preprocessing if already done by job to improve performance
	if config.Get("_rdbmsPreprocessed") != nil {
		logger.Component().WithComponent("CommonRdbmsWriterTask").Debug("Configuration already preprocessed, skipping pretreatment")
		// Fast path: reuse preprocessed configuration
		task.writerJob = NewCommonRdbmsWriterJob(task.dataBaseType)
		task.writerJob.config = config
	} else {
		// Fallback path: perform full initialization with preprocessing
		task.writerJob = NewCommonRdbmsWriterJob(task.dataBaseType)
		if err := task.writerJob.Init(config); err != nil {
			return err
		}
	}

	// 获取表名和列配置（已经过预处理）
	connections := config.GetListConfiguration("parameter.connection")
	if len(connections) > 0 {
		tables := connections[0].GetStringList("table")
		if len(tables) > 0 {
			task.table = tables[0]
		}
	}

	// 获取列配置（此时已经过预处理，不会是"*"）
	task.columns = config.GetStringList("parameter.column")
	task.columnNumber = len(task.columns)

	// 建立数据库连接
	db, err := task.writerJob.createConnection()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}
	task.db = db

	logger.Component().WithComponent("CommonRdbmsWriterTask").Info("Task initialized",
		zap.String("databaseType", task.dataBaseType.String()),
		zap.String("table", task.table),
		zap.Int("columns", task.columnNumber),
		zap.Int("batchSize", task.batchSize))

	return nil
}

// Prepare 准备阶段
func (task *CommonRdbmsWriterTask) Prepare() error {
	return nil
}

// StartWrite 开始写入数据，对应Java版本的startWrite方法
func (task *CommonRdbmsWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	return task.StartWriteWithContext(recordReceiver, context.Background())
}

// StartWriteWithContext 支持Context取消的写入方法
func (task *CommonRdbmsWriterTask) StartWriteWithContext(recordReceiver plugin.RecordReceiver, ctx context.Context) error {
	taskLogger := logger.TaskLoggerFromContext(ctx)
	metricsLogger := logger.Metrics("CommonRdbmsWriterTask")

	// 初始化性能指标
	metrics := logger.NewTaskMetrics()
	metricsLogger.LogTaskStart("Writer", task.dataBaseType.String())

	taskLogger.Info("Starting write task", zap.String("databaseType", task.dataBaseType.String()))

	// 准备批量插入
	batch := make([]element.Record, 0, task.batchSize)
	totalCount := 0
	receiveCount := 0

	defer func() {
		metrics.RecordsRead = int64(receiveCount)
		metrics.RecordsWrite = int64(totalCount)
		metricsLogger.LogTaskComplete("Writer", task.dataBaseType.String(), metrics)
		taskLogger.Info("Shutting down write task", zap.String("databaseType", task.dataBaseType.String()))
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	taskLogger.Info("Entering receive loop", zap.Int("batchSize", task.batchSize))

	for {
		// 检查Context是否被取消
		select {
		case <-ctx.Done():
			taskLogger.Warn("Task cancelled by context", zap.Int("recordsReceived", receiveCount))
			// 处理剩余批次数据
			if len(batch) > 0 {
				taskLogger.Info("Processing remaining batch before cancellation", zap.Int("batchSize", len(batch)))
				if err := task.writeBatch(batch); err != nil {
					taskLogger.Error("Failed to write remaining batch", zap.Error(err))
				}
				totalCount += len(batch)
			}
			return ctx.Err()
		default:
			// 继续处理
		}

		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == coreplugin.ErrChannelClosed {
				taskLogger.Info("Received channel closed signal", zap.Int("recordsReceived", receiveCount))
				// 处理最后一批数据
				if len(batch) > 0 {
					taskLogger.Info("Processing final batch", zap.Int("batchSize", len(batch)))
					if err := task.writeBatch(batch); err != nil {
						return err
					}
					totalCount += len(batch)
				}
				break
			}
			taskLogger.Error("Failed to receive record", zap.Error(err))
			return fmt.Errorf("failed to receive record: %v", err)
		}

		receiveCount++

		// 关键验证：对应Java版本的列数匹配检查
		if record.GetColumnNumber() != task.columnNumber {
			return fmt.Errorf("column configuration error: source record has %d columns but target table requires %d columns. Please check your configuration",
				record.GetColumnNumber(), task.columnNumber)
		}

		batch = append(batch, record)

		if receiveCount%10000 == 0 {
			taskLogger.Info("Progress update", zap.Int("recordsReceived", receiveCount))
		}

		// 达到批次大小时写入
		if len(batch) >= task.batchSize {
			if err := task.writeBatch(batch); err != nil {
				return err
			}
			totalCount += len(batch)
			batch = batch[:0] // 清空batch
		}
	}

	taskLogger.Info("Write task completed",
		zap.String("databaseType", task.dataBaseType.String()),
		zap.Int("recordsReceived", receiveCount),
		zap.Int("recordsWritten", totalCount))
	return nil
}

// writeBatch 批量写入数据
func (task *CommonRdbmsWriterTask) writeBatch(records []element.Record) error {
	if len(records) == 0 {
		return nil
	}

	// 性能计时
	metricsLogger := logger.Metrics("CommonRdbmsWriterTask")
	timer := metricsLogger.StartTimer("BatchWrite")

	// 构建批量插入SQL
	insertSQL, values, err := task.buildBatchInsertSQL(records)
	if err != nil {
		return err
	}

	// 执行批量插入
	result := task.db.Exec(insertSQL, values...)
	if result.Error != nil {
		return fmt.Errorf("failed to execute batch insert: %v", result.Error)
	}

	// 计算执行时间
	duration := timer.Stop()

	// 记录插入结果
	rowsAffected := result.RowsAffected
	totalAttempted := int64(len(records))
	skippedCount := totalAttempted - rowsAffected

	// 记录性能指标
	metricsLogger.LogDatabaseMetrics("BatchInsert", rowsAffected, duration)

	compLogger := logger.Component().WithComponent("CommonRdbmsWriterTask")
	if skippedCount > 0 {
		compLogger.Info("Batch written with conflicts",
			zap.Int64("inserted", rowsAffected),
			zap.Int64("skipped", skippedCount),
			zap.Int64("attempted", totalAttempted),
			zap.Duration("duration", duration))
	} else {
		compLogger.Debug("Batch written successfully",
			zap.Int64("inserted", rowsAffected),
			zap.Int64("total", totalAttempted),
			zap.Duration("duration", duration))
	}

	return nil
}

// buildBatchInsertSQL generates database-specific bulk insert statements
// with proper placeholder formatting and conflict resolution.
func (task *CommonRdbmsWriterTask) buildBatchInsertSQL(records []element.Record) (string, []interface{}, error) {
	columnStr := strings.Join(task.columns, ", ")

	// Build parameterized VALUES clause for batch insert
	valuePlaceholders := make([]string, len(records))
	allValues := make([]interface{}, 0, len(records)*len(task.columns))
	placeholderIndex := 1

	for i, record := range records {
		columnCount := record.GetColumnNumber()
		if columnCount != task.columnNumber {
			return "", nil, fmt.Errorf("record column count (%d) doesn't match config columns (%d)",
				columnCount, task.columnNumber)
		}

		// Generate database-specific placeholders for each record
		recordPlaceholders := make([]string, columnCount)
		for j := 0; j < columnCount; j++ {
			// Database-specific placeholder syntax ($ for PostgreSQL, ? for others)
			switch task.dataBaseType {
			case PostgreSQL:
				recordPlaceholders[j] = fmt.Sprintf("$%d", placeholderIndex)
			case MySQL:
				recordPlaceholders[j] = "?"
			default:
				recordPlaceholders[j] = "?"
			}
			placeholderIndex++

			column := record.GetColumn(j)
			value := task.convertColumnToValue(column)
			allValues = append(allValues, value)
		}

		valuePlaceholders[i] = fmt.Sprintf("(%s)", strings.Join(recordPlaceholders, ", "))
	}

	// Generate database-specific INSERT with conflict resolution
	var insertSQL string
	switch task.dataBaseType {
	case PostgreSQL:
		insertSQL = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING",
			task.table, columnStr, strings.Join(valuePlaceholders, ", "))
	case MySQL:
		insertSQL = fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
			task.table, columnStr, strings.Join(valuePlaceholders, ", "))
	default:
		insertSQL = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			task.table, columnStr, strings.Join(valuePlaceholders, ", "))
	}

	return insertSQL, allValues, nil
}

// convertColumnToValue transforms DataX column types to database-compatible values.
func (task *CommonRdbmsWriterTask) convertColumnToValue(column element.Column) interface{} {
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

	// Fallback to string representation for unknown types
	return column.GetAsString()
}

// Post performs task-level cleanup operations.
func (task *CommonRdbmsWriterTask) Post() error {
	return nil
}

// Destroy closes database connections and releases resources.
func (task *CommonRdbmsWriterTask) Destroy() error {
	if task.db != nil {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}
	return nil
}