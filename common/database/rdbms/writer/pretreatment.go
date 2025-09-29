package writer

import (
	"fmt"
	"strings"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// DatabaseType enumerates supported database types for unified RDBMS operations.
type DatabaseType int

const (
	PostgreSQL DatabaseType = iota
	MySQL
	SQLite
	SQLServer
	Oracle
	GaussDB
)

// String returns the human-readable name for the database type.
func (dt DatabaseType) String() string {
	switch dt {
	case PostgreSQL:
		return "PostgreSQL"
	case MySQL:
		return "MySQL"
	case SQLite:
		return "SQLite"
	case SQLServer:
		return "SQLServer"
	case Oracle:
		return "Oracle"
	case GaussDB:
		return "GaussDB"
	default:
		return "Unknown"
	}
}

// ConnectionFactory provides database connection abstraction for different RDBMS types.
type ConnectionFactory interface {
	GetConnection() (*gorm.DB, error)
	GetConnectionInfo() string
}

// DoPretreatment validates and preprocesses configuration for RDBMS writers.
// Key responsibility: resolves wildcard columns and validates schema compatibility.
func DoPretreatment(originalConfig config.Configuration, dbType DatabaseType) error {
	compLogger := logger.Component().WithComponent("RDBMSPretreatment")
	compLogger.Info("Starting RDBMS pretreatment", zap.String("databaseType", dbType.String()))

	if err := checkRequiredConfig(originalConfig); err != nil {
		return err
	}

	if err := checkBatchSize(originalConfig); err != nil {
		return err
	}

	if err := simplifyConnectionConfig(originalConfig); err != nil {
		return err
	}

	// Critical step: resolve wildcard columns and validate schema compatibility
	if err := dealColumnConf(originalConfig); err != nil {
		return err
	}

	compLogger.Info("RDBMS pretreatment completed successfully", zap.String("databaseType", dbType.String()))
	return nil
}

// checkRequiredConfig validates mandatory configuration parameters.
func checkRequiredConfig(config config.Configuration) error {
	username := config.GetString("parameter.username")
	if username == "" {
		return fmt.Errorf("username is required")
	}

	password := config.GetString("parameter.password")
	if password == "" {
		return fmt.Errorf("password is required")
	}

	return nil
}

// checkBatchSize validates and normalizes batch size configuration.
func checkBatchSize(config config.Configuration) error {
	batchSize := config.GetIntWithDefault("parameter.batchSize", 1024)
	if batchSize < 1 {
		return fmt.Errorf("batchSize must be greater than 0, got: %d", batchSize)
	}

	config.Set("parameter.batchSize", batchSize)
	return nil
}

// simplifyConnectionConfig validates connection parameters and table configuration.
func simplifyConnectionConfig(config config.Configuration) error {
	connections := config.GetListConfiguration("parameter.connection")
	if len(connections) == 0 {
		return fmt.Errorf("connection configuration is required")
	}

	// Process primary connection configuration
	conn := connections[0]
	jdbcUrl := conn.GetString("jdbcUrl")
	if jdbcUrl == "" {
		return fmt.Errorf("jdbcUrl is required")
	}

	tables := conn.GetStringList("table")
	if len(tables) == 0 {
		return fmt.Errorf("table configuration is required")
	}

	// Track table count for validation purposes
	config.Set("tableNumber", len(tables))

	return nil
}

// dealColumnConf resolves wildcard column specifications to concrete column lists.
// This is critical for preventing schema mismatches between source and target.
func dealColumnConf(config config.Configuration) error {
	compLogger := logger.Component().WithComponent("RDBMSPretreatment")
	compLogger.Info("Processing column configuration")

	userConfiguredColumns := config.GetStringList("parameter.column")

	// 如果GetStringList返回空，尝试直接获取原始值
	if len(userConfiguredColumns) == 0 {
		rawColumn := config.Get("parameter.column")

		// 尝试强制类型转换
		if columnList, ok := rawColumn.([]interface{}); ok {
			userConfiguredColumns = make([]string, len(columnList))
			for i, item := range columnList {
				userConfiguredColumns[i] = fmt.Sprintf("%v", item)
			}
		} else if columnList, ok := rawColumn.([]string); ok {
			userConfiguredColumns = columnList
		}
	}

	if len(userConfiguredColumns) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	// 关键逻辑：处理 "*" 配置
	if len(userConfiguredColumns) == 1 && userConfiguredColumns[0] == "*" {
		compLogger.Info("Found column configuration with '*', will replace with actual target table columns")

		// 获取目标表的所有列
		allColumns, err := getTargetTableColumns(config)
		if err != nil {
			return fmt.Errorf("failed to get target table columns: %v", err)
		}

		if len(allColumns) == 0 {
			return fmt.Errorf("no columns found in target table")
		}

		// 发出风险警告（对应Java版本的LOG.warn）
		compLogger.Warn("Using '*' for column configuration carries risks",
			zap.String("warning", "When target table schema changes, it may affect task correctness or cause errors. Please consider using explicit column names."))

		// 关键步骤：回填实际列名，对应Java版本的 originalConfig.set(Key.COLUMN, allColumns)
		config.Set("parameter.column", allColumns)
		compLogger.Info("Replaced '*' with actual target table columns", zap.Strings("columns", allColumns))

	} else {
		// 如果已经是具体列名（可能是之前预处理的结果），进行轻量级验证
		if len(userConfiguredColumns) > 50 { // 如果列数过多，可能有问题
			compLogger.Warn("Large number of columns detected, please verify configuration", zap.Int("columnCount", len(userConfiguredColumns)))
		}
	}

	return nil
}

// getTargetTableColumns queries the target table schema to retrieve column names.
func getTargetTableColumns(config config.Configuration) ([]string, error) {
	// 获取数据库连接参数
	username := config.GetString("parameter.username")
	password := config.GetString("parameter.password")

	connections := config.GetListConfiguration("parameter.connection")
	if len(connections) == 0 {
		return nil, fmt.Errorf("no connection configuration found")
	}

	conn := connections[0]
	jdbcUrl := conn.GetString("jdbcUrl")
	tables := conn.GetStringList("table")
	if len(tables) == 0 {
		return nil, fmt.Errorf("no table configuration found")
	}

	table := tables[0] // 使用第一个表

	// 根据JDBC URL确定数据库类型和连接方式
	dbType, err := detectDatabaseTypeFromJDBC(jdbcUrl)
	if err != nil {
		return nil, err
	}

	// 创建数据库连接
	db, err := createConnectionFromJDBC(dbType, jdbcUrl, username, password)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}
	defer func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	// 查询表的列信息
	columns, err := queryTableColumns(db, dbType, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query table columns: %v", err)
	}

	compLogger := logger.Component().WithComponent("RDBMSPretreatment")
	compLogger.Info("Retrieved columns from target table",
		zap.Int("columnCount", len(columns)),
		zap.String("table", table),
		zap.Strings("columns", columns))
	return columns, nil
}

// detectDatabaseTypeFromJDBC identifies database type from JDBC connection string.
func detectDatabaseTypeFromJDBC(jdbcUrl string) (DatabaseType, error) {
	if strings.Contains(jdbcUrl, "postgresql") {
		return PostgreSQL, nil
	} else if strings.Contains(jdbcUrl, "mysql") {
		return MySQL, nil
	} else if strings.Contains(jdbcUrl, "sqlite") {
		return SQLite, nil
	} else if strings.Contains(jdbcUrl, "sqlserver") {
		return SQLServer, nil
	} else if strings.Contains(jdbcUrl, "oracle") {
		return Oracle, nil
	} else if strings.Contains(jdbcUrl, "gaussdb") || strings.Contains(jdbcUrl, "opengauss") {
		return GaussDB, nil
	}
	return PostgreSQL, fmt.Errorf("unsupported database type in JDBC URL: %s", jdbcUrl)
}

// createConnectionFromJDBC establishes database connection using appropriate driver.
func createConnectionFromJDBC(dbType DatabaseType, jdbcUrl, username, password string) (*gorm.DB, error) {
	switch dbType {
	case PostgreSQL:
		return createPostgreSQLConnection(jdbcUrl, username, password)
	case MySQL:
		return createMySQLConnection(jdbcUrl, username, password)
	case GaussDB:
		// GaussDB uses PostgreSQL protocol
		return createPostgreSQLConnection(jdbcUrl, username, password)
	// TODO: 添加其他数据库类型的支持
	default:
		return nil, fmt.Errorf("database type %s not yet supported in common layer", dbType.String())
	}
}

// validateUserColumns verifies that user-specified columns exist in the target table.
func validateUserColumns(config config.Configuration, userColumns []string) error {
	// 获取目标表的所有列
	allColumns, err := getTargetTableColumns(config)
	if err != nil {
		return err
	}

	// 检查用户配置的列数是否超过目标表列数
	if len(userColumns) > len(allColumns) {
		return fmt.Errorf("configured column count (%d) exceeds target table column count (%d)",
			len(userColumns), len(allColumns))
	}

	// 检查每个用户配置的列是否存在于目标表中
	allColumnsMap := make(map[string]bool)
	for _, col := range allColumns {
		allColumnsMap[col] = true
	}

	for _, userCol := range userColumns {
		if !allColumnsMap[userCol] {
			return fmt.Errorf("column '%s' does not exist in target table", userCol)
		}
	}

	compLogger := logger.Component().WithComponent("RDBMSPretreatment")
	compLogger.Info("Validated user configured columns", zap.Strings("columns", userColumns))
	return nil
}