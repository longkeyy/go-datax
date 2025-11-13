package writer

import (
	"fmt"
	"strings"

	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// createPostgreSQLConnection 创建PostgreSQL数据库连接
func createPostgreSQLConnection(jdbcUrl, username, password string) (*gorm.DB, error) {
	dsn, err := convertPostgreSQLJdbcUrl(jdbcUrl, username, password)
	if err != nil {
		return nil, err
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %v", err)
	}

	return db, nil
}

// createMySQLConnection 创建MySQL数据库连接
func createMySQLConnection(jdbcUrl, username, password string) (*gorm.DB, error) {
	dsn, err := convertMySQLJdbcUrl(jdbcUrl, username, password)
	if err != nil {
		return nil, err
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %v", err)
	}

	return db, nil
}

// convertPostgreSQLJdbcUrl 转换PostgreSQL JDBC URL为GORM DSN
func convertPostgreSQLJdbcUrl(jdbcUrl, username, password string) (string, error) {
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
		username,
		password,
		database)

	return dsn, nil
}

// convertMySQLJdbcUrl 转换MySQL JDBC URL为GORM DSN
func convertMySQLJdbcUrl(jdbcUrl, username, password string) (string, error) {
	// 解析JDBC URL: jdbc:mysql://host:port/database
	if !strings.HasPrefix(jdbcUrl, "jdbc:mysql://") {
		return "", fmt.Errorf("invalid MySQL JDBC URL: %s", jdbcUrl)
	}

	// 移除jdbc:mysql://前缀
	url := strings.TrimPrefix(jdbcUrl, "jdbc:mysql://")

	// 分离host:port和database
	parts := strings.Split(url, "/")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid JDBC URL format: %s", jdbcUrl)
	}

	hostPort := parts[0]
	database := parts[1]

	// 构建MySQL连接字符串
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		username,
		password,
		hostPort,
		database)

	return dsn, nil
}

// queryTableColumns 查询表的列信息
func queryTableColumns(db *gorm.DB, dbType DatabaseType, table string) ([]string, error) {
	switch dbType {
	case PostgreSQL:
		return queryPostgreSQLTableColumns(db, table)
	case MySQL:
		return queryMySQLTableColumns(db, table)
	default:
		return nil, fmt.Errorf("database type %s not supported for column query", dbType.String())
	}
}

// queryPostgreSQLTableColumns 查询PostgreSQL表的列信息
func queryPostgreSQLTableColumns(db *gorm.DB, table string) ([]string, error) {
	query := `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = 'public'
		AND table_name = ?
		ORDER BY ordinal_position`

	rows, err := db.Raw(query, table).Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to query PostgreSQL table columns: %v", err)
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
		return nil, fmt.Errorf("no columns found for PostgreSQL table %s", table)
	}

	return columns, nil
}

// queryMySQLTableColumns 查询MySQL表的列信息
func queryMySQLTableColumns(db *gorm.DB, table string) ([]string, error) {
	query := `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = DATABASE()
		AND table_name = ?
		ORDER BY ordinal_position`

	rows, err := db.Raw(query, table).Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to query MySQL table columns: %v", err)
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
		return nil, fmt.Errorf("no columns found for MySQL table %s", table)
	}

	return columns, nil
}

// queryPrimaryKeyColumns queries the primary key columns of a table
// Returns: array of primary key column names (may be empty if no primary key exists)
// Supports composite primary keys (multiple columns)
func queryPrimaryKeyColumns(db *gorm.DB, dbType DatabaseType, table string) ([]string, error) {
	switch dbType {
	case PostgreSQL:
		return queryPostgreSQLPrimaryKeys(db, table)
	case MySQL:
		return queryMySQLPrimaryKeys(db, table)
	default:
		return nil, fmt.Errorf("database type %s not supported for primary key query", dbType.String())
	}
}

// queryPostgreSQLPrimaryKeys queries primary key columns for PostgreSQL table
func queryPostgreSQLPrimaryKeys(db *gorm.DB, table string) ([]string, error) {
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid
			AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass
		  AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)`

	rows, err := db.Raw(query, table).Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to query PostgreSQL primary keys: %v", err)
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %v", err)
		}
		primaryKeys = append(primaryKeys, columnName)
	}

	// Empty array is valid (table has no primary key)
	return primaryKeys, nil
}

// queryMySQLPrimaryKeys queries primary key columns for MySQL table
func queryMySQLPrimaryKeys(db *gorm.DB, table string) ([]string, error) {
	query := `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = DATABASE()
		  AND table_name = ?
		  AND column_key = 'PRI'
		ORDER BY ordinal_position`

	rows, err := db.Raw(query, table).Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to query MySQL primary keys: %v", err)
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %v", err)
		}
		primaryKeys = append(primaryKeys, columnName)
	}

	// Empty array is valid (table has no primary key)
	return primaryKeys, nil
}