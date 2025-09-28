package oraclereader

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/rdbms/writer"
	"go.uber.org/zap"

	// 使用go-ora纯Go驱动，无需Oracle客户端库
	_ "github.com/sijms/go-ora/v2"
)

const (
	DefaultBatchSize = 1024
	DefaultFetchSize = 1024
)

// OracleReaderJob Oracle读取作业
type OracleReaderJob struct {
	config   *config.Configuration
	username string
	password string
	jdbcUrl  string
	tables   []string
	columns  []string
	splitPk  string
	where    string
	querySql string
	session  []string
	hint     string
}

func NewOracleReaderJob() *OracleReaderJob {
	return &OracleReaderJob{}
}

func (job *OracleReaderJob) Init(config *config.Configuration) error {
	job.config = config
	compLogger := logger.Component().WithComponent("OracleReaderJob")

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
	job.tables = conn.GetStringList("table")

	if job.jdbcUrl == "" || len(job.tables) == 0 {
		return fmt.Errorf("jdbcUrl and table are required")
	}

	// 获取列信息
	job.columns = config.GetStringList("parameter.column")
	if len(job.columns) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	// 获取可选参数
	job.splitPk = config.GetString("parameter.splitPk")
	job.where = config.GetString("parameter.where")
	job.querySql = config.GetString("parameter.querySql")
	job.session = config.GetStringList("parameter.session")
	job.hint = config.GetString("parameter.hint")

	// 执行预处理
	err := writer.DoPretreatment(config, writer.Oracle)
	if err != nil {
		return fmt.Errorf("pretreatment failed: %v", err)
	}

	compLogger.Info("OracleReader initialized",
		zap.String("jdbcUrl", job.jdbcUrl),
		zap.Strings("tables", job.tables),
		zap.Int("columns", len(job.columns)))
	return nil
}

func (job *OracleReaderJob) Prepare() error {
	return nil
}

func (job *OracleReaderJob) Split(adviceNumber int) ([]*config.Configuration, error) {
	taskConfigs := make([]*config.Configuration, 0)
	compLogger := logger.Component().WithComponent("OracleReaderJob")

	for _, table := range job.tables {
		if job.splitPk != "" && adviceNumber > 1 {
			// 基于splitPk进行分片
			ranges, err := job.calculateSplitRanges(table, adviceNumber)
			if err != nil {
				compLogger.Warn("Failed to calculate split ranges, fallback to single task", zap.Error(err))
				taskConfig := job.config.Clone()
				taskConfig.Set("taskId", 0)
				taskConfig.Set("parameter.table", table)
				taskConfigs = append(taskConfigs, taskConfig)
			} else {
				for i, rangeCondition := range ranges {
					taskConfig := job.config.Clone()
					taskConfig.Set("taskId", i)
					taskConfig.Set("parameter.table", table)

					// 合并where条件
					whereCondition := job.where
					if whereCondition != "" && rangeCondition != "" {
						whereCondition = fmt.Sprintf("(%s) AND (%s)", whereCondition, rangeCondition)
					} else if rangeCondition != "" {
						whereCondition = rangeCondition
					}
					taskConfig.Set("parameter.where", whereCondition)
					taskConfigs = append(taskConfigs, taskConfig)
				}
			}
		} else {
			// 不进行分片
			taskConfig := job.config.Clone()
			taskConfig.Set("taskId", 0)
			taskConfig.Set("parameter.table", table)
			taskConfigs = append(taskConfigs, taskConfig)
		}
	}

	compLogger.Info("Split tasks completed",
		zap.Int("taskCount", len(taskConfigs)),
		zap.String("splitPk", job.splitPk))
	return taskConfigs, nil
}

func (job *OracleReaderJob) calculateSplitRanges(table string, adviceNumber int) ([]string, error) {
	db, err := job.connect()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// 查询splitPk的最小值和最大值
	minMaxQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", job.splitPk, job.splitPk, table)
	if job.where != "" {
		minMaxQuery += " WHERE " + job.where
	}

	row := db.QueryRow(minMaxQuery)
	var minVal, maxVal interface{}
	if err := row.Scan(&minVal, &maxVal); err != nil {
		return nil, err
	}

	if minVal == nil || maxVal == nil {
		return []string{""}, nil // 返回空的分片条件
	}

	// 根据数据类型进行分片
	return job.generateSplitConditions(minVal, maxVal, adviceNumber)
}

func (job *OracleReaderJob) generateSplitConditions(minVal, maxVal interface{}, adviceNumber int) ([]string, error) {
	var conditions []string

	switch v := minVal.(type) {
	case int64:
		minInt := v
		maxInt := maxVal.(int64)
		step := (maxInt - minInt) / int64(adviceNumber)
		if step == 0 {
			step = 1
		}

		for i := 0; i < adviceNumber; i++ {
			start := minInt + int64(i)*step
			var end int64
			if i == adviceNumber-1 {
				end = maxInt
			} else {
				end = start + step - 1
			}

			condition := fmt.Sprintf("%s >= %d AND %s <= %d", job.splitPk, start, job.splitPk, end)
			conditions = append(conditions, condition)
		}
	case float64:
		minFloat := v
		maxFloat := maxVal.(float64)
		step := (maxFloat - minFloat) / float64(adviceNumber)

		for i := 0; i < adviceNumber; i++ {
			start := minFloat + float64(i)*step
			var end float64
			if i == adviceNumber-1 {
				end = maxFloat
			} else {
				end = start + step
			}

			condition := fmt.Sprintf("%s >= %f AND %s < %f", job.splitPk, start, job.splitPk, end)
			conditions = append(conditions, condition)
		}
	case string:
		// 对于字符串类型，简单使用范围查询
		minStr := v
		maxStr := maxVal.(string)
		condition := fmt.Sprintf("%s >= '%s' AND %s <= '%s'", job.splitPk, minStr, job.splitPk, maxStr)
		conditions = append(conditions, condition)
	default:
		// 不支持的类型，返回单个空条件
		return []string{""}, nil
	}

	return conditions, nil
}

func (job *OracleReaderJob) connect() (*sql.DB, error) {
	dsn, err := job.parseJdbcUrl()
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open Oracle connection: %v", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping Oracle: %v", err)
	}

	return db, nil
}

func (job *OracleReaderJob) parseJdbcUrl() (string, error) {
	// 支持多种Oracle连接字符串格式
	// oracle://user:password@host:port/service
	// oracle://user:password@host:port:sid
	jdbcUrl := job.jdbcUrl

	// 移除oracle://前缀
	if strings.HasPrefix(jdbcUrl, "oracle://") {
		jdbcUrl = strings.TrimPrefix(jdbcUrl, "oracle://")
	}

	// 构建go-ora连接字符串格式
	// oracle://user:password@host:port/service_name
	return fmt.Sprintf("oracle://%s:%s@%s", job.username, job.password, jdbcUrl), nil
}

func (job *OracleReaderJob) Post() error {
	return nil
}

func (job *OracleReaderJob) Destroy() error {
	return nil
}

// OracleReaderTask Oracle读取任务
type OracleReaderTask struct {
	config    *config.Configuration
	readerJob *OracleReaderJob
	db        *sql.DB
	table     string
}

func NewOracleReaderTask() *OracleReaderTask {
	return &OracleReaderTask{}
}

func (task *OracleReaderTask) Init(config *config.Configuration) error {
	task.config = config
	compLogger := logger.Component().WithComponent("OracleReaderTask")

	// 创建ReaderJob来重用配置逻辑
	task.readerJob = NewOracleReaderJob()
	err := task.readerJob.Init(config)
	if err != nil {
		return err
	}

	// 获取任务特定的表名
	task.table = config.GetString("parameter.table")
	if task.table == "" {
		// 使用第一个表作为默认值
		if len(task.readerJob.tables) > 0 {
			task.table = task.readerJob.tables[0]
		}
	}

	// 建立数据库连接
	task.db, err = task.readerJob.connect()
	if err != nil {
		return err
	}

	// 执行session SQL
	err = task.executeSessionSql()
	if err != nil {
		return err
	}

	compLogger.Info("OracleReader task initialized", zap.String("table", task.table))
	return nil
}

func (task *OracleReaderTask) executeSessionSql() error {
	for _, sessionSql := range task.readerJob.session {
		if sessionSql != "" {
			_, err := task.db.Exec(sessionSql)
			if err != nil {
				return fmt.Errorf("failed to execute session SQL '%s': %v", sessionSql, err)
			}
		}
	}
	return nil
}

func (task *OracleReaderTask) Prepare() error {
	return nil
}

func (task *OracleReaderTask) StartRead(recordSender plugin.RecordSender) error {
	compLogger := logger.Component().WithComponent("OracleReaderTask")

	defer func() {
		if task.db != nil {
			task.db.Close()
		}
	}()

	// 构建查询SQL
	querySql, err := task.buildQuery()
	if err != nil {
		return err
	}

	compLogger.Info("Executing query", zap.String("query", querySql))

	// 执行查询
	rows, err := task.db.Query(querySql)
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

	recordCount := int64(0)
	fetchSize := task.config.GetIntWithDefault("parameter.fetchSize", DefaultFetchSize)

	// 逐行处理数据
	for rows.Next() {
		record := element.NewRecord()

		// 创建扫描目标
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 扫描行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		// 转换为DataX记录
		for i, value := range values {
			column, err := task.convertValue(value, columnTypes[i])
			if err != nil {
				return fmt.Errorf("failed to convert value at column %s: %v", columns[i], err)
			}
			record.AddColumn(column)
		}

		// 发送记录
		if err := recordSender.SendRecord(record); err != nil {
			return fmt.Errorf("failed to send record: %v", err)
		}

		recordCount++

		// 每fetchSize条记录输出一次进度
		if recordCount%int64(fetchSize) == 0 {
			compLogger.Debug("Reading progress", zap.Int64("records", recordCount))
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error: %v", err)
	}

	compLogger.Info("Read task completed", zap.Int64("totalRecords", recordCount))
	return nil
}

func (task *OracleReaderTask) buildQuery() (string, error) {
	// 如果指定了querySql，直接使用
	if task.readerJob.querySql != "" {
		return task.readerJob.querySql, nil
	}

	// 处理列配置
	columns := task.readerJob.columns
	if len(columns) == 1 && columns[0] == "*" {
		// 获取表的实际列名
		actualColumns, err := task.getTableColumns()
		if err != nil {
			return "", fmt.Errorf("failed to get table columns: %v", err)
		}
		columns = actualColumns
	}

	columnStr := strings.Join(columns, ", ")

	// 构建基本查询
	var querySql string
	if task.readerJob.hint != "" {
		querySql = fmt.Sprintf("SELECT /*+ %s */ %s FROM %s", task.readerJob.hint, columnStr, task.table)
	} else {
		querySql = fmt.Sprintf("SELECT %s FROM %s", columnStr, task.table)
	}

	// 添加WHERE条件
	whereCondition := task.config.GetString("parameter.where")
	if whereCondition != "" {
		querySql += " WHERE " + whereCondition
	}

	return querySql, nil
}

func (task *OracleReaderTask) getTableColumns() ([]string, error) {
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

func (task *OracleReaderTask) convertValue(value interface{}, columnType *sql.ColumnType) (element.Column, error) {
	if value == nil {
		return element.NewStringColumn(""), nil
	}

	typeName := strings.ToUpper(columnType.DatabaseTypeName())

	switch typeName {
	case "NUMBER", "INTEGER", "INT", "SMALLINT", "BIGINT":
		switch v := value.(type) {
		case int64:
			return element.NewLongColumn(v), nil
		case float64:
			// Oracle NUMBER类型可能返回float64
			if v == float64(int64(v)) {
				return element.NewLongColumn(int64(v)), nil
			}
			return element.NewDoubleColumn(v), nil
		case string:
			if intVal, err := strconv.ParseInt(v, 10, 64); err == nil {
				return element.NewLongColumn(intVal), nil
			}
			if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
				return element.NewDoubleColumn(floatVal), nil
			}
			return element.NewStringColumn(v), nil
		default:
			return element.NewStringColumn(fmt.Sprintf("%v", v)), nil
		}

	case "NUMERIC", "DECIMAL", "FLOAT", "DOUBLE PRECISION", "REAL":
		switch v := value.(type) {
		case float64:
			return element.NewDoubleColumn(v), nil
		case int64:
			return element.NewDoubleColumn(float64(v)), nil
		case string:
			if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
				return element.NewDoubleColumn(floatVal), nil
			}
			return element.NewStringColumn(v), nil
		default:
			return element.NewStringColumn(fmt.Sprintf("%v", v)), nil
		}

	case "CHAR", "NCHAR", "VARCHAR", "VARCHAR2", "NVARCHAR2", "CLOB", "NCLOB", "LONG":
		return element.NewStringColumn(fmt.Sprintf("%v", value)), nil

	case "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE":
		switch v := value.(type) {
		case time.Time:
			return element.NewDateColumn(v), nil
		case string:
			// 尝试解析常见的时间格式
			timeFormats := []string{
				"2006-01-02 15:04:05",
				"2006-01-02T15:04:05Z",
				"2006-01-02T15:04:05.000Z",
				"2006-01-02",
			}
			for _, format := range timeFormats {
				if t, err := time.Parse(format, v); err == nil {
					return element.NewDateColumn(t), nil
				}
			}
			return element.NewStringColumn(v), nil
		default:
			return element.NewStringColumn(fmt.Sprintf("%v", v)), nil
		}

	case "BLOB", "BFILE", "RAW", "LONG RAW":
		switch v := value.(type) {
		case []byte:
			return element.NewBytesColumn(v), nil
		default:
			return element.NewStringColumn(fmt.Sprintf("%v", value)), nil
		}

	default:
		return element.NewStringColumn(fmt.Sprintf("%v", value)), nil
	}
}

func (task *OracleReaderTask) Post() error {
	return nil
}

func (task *OracleReaderTask) Destroy() error {
	if task.db != nil {
		return task.db.Close()
	}
	return nil
}