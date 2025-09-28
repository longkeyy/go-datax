package mysqlreader

import (
	"database/sql"
	"fmt"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"log"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	DefaultFetchSize = 1024
)

// MySQLReaderJob MySQL读取作业
type MySQLReaderJob struct {
	config   *config.Configuration
	username string
	password string
	jdbcUrls []string
	tables   []string
	columns  []string
	where    string
	splitPk  string
	querySql string // 支持自定义查询SQL
}

func NewMySQLReaderJob() *MySQLReaderJob {
	return &MySQLReaderJob{}
}

func (job *MySQLReaderJob) Init(config *config.Configuration) error {
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

	if len(job.jdbcUrls) == 0 {
		return fmt.Errorf("jdbcUrl is required")
	}

	// 获取可选参数
	job.where = config.GetString("parameter.where")
	job.splitPk = config.GetString("parameter.splitPk")
	job.querySql = config.GetString("parameter.querySql")

	// 检查配置模式：querySql 或 table/column 模式
	if job.querySql != "" {
		// querySql模式：直接使用用户提供的SQL查询
		log.Printf("MySQL Reader initialized with querySql mode")
	} else {
		// table/column模式：需要table和column配置
		job.tables = conn.GetStringList("table")
		job.columns = config.GetStringList("parameter.column")

		if len(job.tables) == 0 {
			return fmt.Errorf("table is required when querySql is not specified")
		}
		if len(job.columns) == 0 {
			return fmt.Errorf("column is required when querySql is not specified")
		}
		log.Printf("MySQL Reader initialized with table/column mode: tables=%v, columns=%v", job.tables, job.columns)
	}

	return nil
}

func (job *MySQLReaderJob) Split(adviceNumber int) ([]*config.Configuration, error) {
	taskConfigs := make([]*config.Configuration, 0)

	// 如果使用querySql模式或没有设置splitPk，则不进行分片，使用单个任务
	if job.querySql != "" || job.splitPk == "" {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", 0)
		taskConfigs = append(taskConfigs, taskConfig)
		if job.querySql != "" {
			log.Printf("Using querySql mode, single task")
		} else {
			log.Printf("No splitPk specified, using single task")
		}
		return taskConfigs, nil
	}

	// 如果设置了splitPk，尝试进行分片
	ranges, err := job.calculateSplitRanges(adviceNumber)
	if err != nil {
		log.Printf("Failed to calculate split ranges, fallback to single task: %v", err)
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", 0)
		taskConfigs = append(taskConfigs, taskConfig)
		return taskConfigs, nil
	}

	// 创建分片任务配置
	for i, splitRange := range ranges {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfig.Set("parameter.splitRange", splitRange)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	log.Printf("Split into %d tasks based on splitPk: %s", len(taskConfigs), job.splitPk)
	return taskConfigs, nil
}

func (job *MySQLReaderJob) calculateSplitRanges(adviceNumber int) ([]map[string]interface{}, error) {
	// 连接数据库
	db, err := job.connect()
	if err != nil {
		return nil, err
	}
	defer func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	// 检测splitPk列的数据类型
	columnType, err := job.detectColumnType(db, job.tables[0], job.splitPk)
	if err != nil {
		return nil, fmt.Errorf("failed to detect column type for splitPk '%s': %v", job.splitPk, err)
	}

	// 根据数据类型选择切分策略
	if job.isNumericType(columnType) {
		return job.calculateNumericSplitRanges(db, adviceNumber)
	} else if job.isTextType(columnType) {
		return job.calculateTextSplitRanges(db, adviceNumber, columnType)
	} else {
		return nil, fmt.Errorf("unsupported column type '%s' for splitPk. Supported types: integer, varchar, text", columnType)
	}
}

// detectColumnType 检测列的数据类型（MySQL版本）
func (job *MySQLReaderJob) detectColumnType(db *gorm.DB, table, column string) (string, error) {
	query := `
		SELECT DATA_TYPE
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME = ? AND COLUMN_NAME = ? AND TABLE_SCHEMA = DATABASE()
	`

	var dataType string
	err := db.Raw(query, table, column).Row().Scan(&dataType)
	if err != nil {
		return "", fmt.Errorf("failed to query column type: %v", err)
	}

	return dataType, nil
}

// isNumericType 判断是否为数值类型（MySQL版本）
func (job *MySQLReaderJob) isNumericType(dataType string) bool {
	numericTypes := []string{
		"int", "integer", "bigint", "smallint", "tinyint", "mediumint",
		"decimal", "numeric", "float", "double", "bit",
	}

	for _, t := range numericTypes {
		if strings.EqualFold(dataType, t) {
			return true
		}
	}
	return false
}

// isTextType 判断是否为文本类型（MySQL版本）
func (job *MySQLReaderJob) isTextType(dataType string) bool {
	textTypes := []string{
		"varchar", "char", "text", "tinytext", "mediumtext", "longtext",
		"binary", "varbinary", "enum", "set",
	}

	for _, t := range textTypes {
		if strings.EqualFold(dataType, t) {
			return true
		}
	}
	return false
}

// calculateNumericSplitRanges 计算数值类型的分片范围（MySQL版本）
func (job *MySQLReaderJob) calculateNumericSplitRanges(db *gorm.DB, adviceNumber int) ([]map[string]interface{}, error) {
	table := job.tables[0]
	query := fmt.Sprintf("SELECT MIN(`%s`), MAX(`%s`) FROM `%s`", job.splitPk, job.splitPk, table)
	if job.where != "" {
		query += fmt.Sprintf(" WHERE %s", job.where)
	}

	var minVal, maxVal sql.NullInt64
	err := db.Raw(query).Row().Scan(&minVal, &maxVal)
	if err != nil {
		return nil, err
	}

	if !minVal.Valid || !maxVal.Valid {
		return nil, fmt.Errorf("splitPk column contains null values")
	}

	ranges := make([]map[string]interface{}, 0)
	step := (maxVal.Int64 - minVal.Int64 + int64(adviceNumber) - 1) / int64(adviceNumber)

	if step <= 0 {
		step = 1
	}

	for i := 0; i < adviceNumber; i++ {
		rangeStart := minVal.Int64 + int64(i)*step
		rangeEnd := minVal.Int64 + int64(i+1)*step - 1

		if i == adviceNumber-1 {
			rangeEnd = maxVal.Int64
		}

		ranges = append(ranges, map[string]interface{}{
			"start":     rangeStart,
			"end":       rangeEnd,
			"type":      "numeric",
			"inclusive": true,
		})
	}

	return ranges, nil
}

// calculateTextSplitRanges 计算文本类型的分片范围（MySQL版本）
func (job *MySQLReaderJob) calculateTextSplitRanges(db *gorm.DB, adviceNumber int, columnType string) ([]map[string]interface{}, error) {
	// 策略1: 尝试字典序范围切分
	ranges, err := job.calculateTextDictionarySplitRanges(db, adviceNumber)
	if err != nil {
		log.Printf("Dictionary-based splitting failed, trying offset-based splitting: %v", err)

		// 策略2: 使用OFFSET/LIMIT切分
		ranges, err = job.calculateOffsetSplitRanges(db, adviceNumber)
		if err != nil {
			log.Printf("Offset-based splitting failed, falling back to hash-based splitting: %v", err)
			// 策略3: hash切分（兜底）
			return job.calculateHashSplitRanges(adviceNumber), nil
		}
	}

	return ranges, nil
}

// calculateTextDictionarySplitRanges 基于字典序的文本切分（MySQL版本）
func (job *MySQLReaderJob) calculateTextDictionarySplitRanges(db *gorm.DB, adviceNumber int) ([]map[string]interface{}, error) {
	table := job.tables[0]

	// 获取MIN和MAX值
	query := fmt.Sprintf("SELECT MIN(`%s`), MAX(`%s`) FROM `%s`", job.splitPk, job.splitPk, table)
	if job.where != "" {
		query += fmt.Sprintf(" WHERE %s", job.where)
	}

	var minVal, maxVal sql.NullString
	err := db.Raw(query).Row().Scan(&minVal, &maxVal)
	if err != nil {
		return nil, fmt.Errorf("failed to get min/max values: %v", err)
	}

	if !minVal.Valid || !maxVal.Valid {
		return nil, fmt.Errorf("splitPk column contains null values")
	}

	// 如果只有一个唯一值，使用OFFSET切分
	if minVal.String == maxVal.String {
		return job.calculateOffsetSplitRanges(db, adviceNumber)
	}

	// 采样获取边界值
	sampleQuery := fmt.Sprintf(`
		SELECT DISTINCT %s
		FROM %s
		%s
		ORDER BY %s
		LIMIT %d
	`, fmt.Sprintf("`%s`", job.splitPk), fmt.Sprintf("`%s`", table),
		func() string {
			if job.where != "" {
				return "WHERE " + job.where
			}
			return ""
		}(),
		fmt.Sprintf("`%s`", job.splitPk), adviceNumber*10) // 采样更多值来选择边界

	rows, err := db.Raw(sampleQuery).Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to sample values for dictionary split: %v", err)
	}
	defer rows.Close()

	var sampleValues []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			continue
		}
		sampleValues = append(sampleValues, value)
	}

	if len(sampleValues) < adviceNumber {
		// 样本不足，使用OFFSET切分
		return job.calculateOffsetSplitRanges(db, adviceNumber)
	}

	// 选择边界值
	step := len(sampleValues) / adviceNumber
	if step == 0 {
		step = 1
	}

	ranges := make([]map[string]interface{}, 0)
	for i := 0; i < adviceNumber; i++ {
		var start, end interface{}
		var startOp, endOp string

		if i == 0 {
			start = nil
			startOp = ""
		} else {
			boundaryIdx := i * step
			if boundaryIdx < len(sampleValues) {
				start = sampleValues[boundaryIdx]
				startOp = ">="
			}
		}

		if i == adviceNumber-1 {
			end = nil
			endOp = ""
		} else {
			boundaryIdx := (i + 1) * step
			if boundaryIdx < len(sampleValues) {
				end = sampleValues[boundaryIdx]
				endOp = "<"
			}
		}

		ranges = append(ranges, map[string]interface{}{
			"start":   start,
			"end":     end,
			"startOp": startOp,
			"endOp":   endOp,
			"type":    "text_dictionary",
			"taskId":  i,
		})
	}

	return ranges, nil
}

// calculateOffsetSplitRanges 基于OFFSET/LIMIT的切分（MySQL版本）
func (job *MySQLReaderJob) calculateOffsetSplitRanges(db *gorm.DB, adviceNumber int) ([]map[string]interface{}, error) {
	table := job.tables[0]

	// 获取总行数
	var totalCount int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
	if job.where != "" {
		countQuery += fmt.Sprintf(" WHERE %s", job.where)
	}

	err := db.Raw(countQuery).Row().Scan(&totalCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get total count: %v", err)
	}

	if totalCount == 0 {
		return nil, fmt.Errorf("table is empty")
	}

	// 计算每个分片的OFFSET和LIMIT
	ranges := make([]map[string]interface{}, 0)
	baseLimit := totalCount / int64(adviceNumber)
	remainder := totalCount % int64(adviceNumber)

	var currentOffset int64 = 0

	for i := 0; i < adviceNumber; i++ {
		limit := baseLimit
		if int64(i) < remainder {
			limit++ // 前几个分片多分配一行
		}

		ranges = append(ranges, map[string]interface{}{
			"type":   "offset",
			"offset": currentOffset,
			"limit":  limit,
			"taskId": i,
		})

		currentOffset += limit
	}

	return ranges, nil
}

// calculateHashSplitRanges 基于hash的切分（MySQL版本）
func (job *MySQLReaderJob) calculateHashSplitRanges(adviceNumber int) []map[string]interface{} {
	ranges := make([]map[string]interface{}, 0)

	for i := 0; i < adviceNumber; i++ {
		ranges = append(ranges, map[string]interface{}{
			"type":   "hash",
			"taskId": i,
			"total":  adviceNumber,
		})
	}

	return ranges
}

func (job *MySQLReaderJob) connect() (*gorm.DB, error) {
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

	return db, nil
}

func (job *MySQLReaderJob) convertJdbcUrl(jdbcUrl string) (string, error) {
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

	// 构建MySQL DSN字符串
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&loc=Local",
		job.username,
		job.password,
		hostPort,
		database)

	// 添加额外参数
	if params != "" {
		dsn += "&" + params
	}

	return dsn, nil
}

func (job *MySQLReaderJob) Post() error {
	return nil
}

func (job *MySQLReaderJob) Destroy() error {
	return nil
}

// MySQLReaderTask MySQL读取任务
type MySQLReaderTask struct {
	config    *config.Configuration
	readerJob *MySQLReaderJob
	db        *gorm.DB
}

func NewMySQLReaderTask() *MySQLReaderTask {
	return &MySQLReaderTask{}
}

func (task *MySQLReaderTask) Init(config *config.Configuration) error {
	task.config = config

	// 创建ReaderJob来重用连接逻辑
	task.readerJob = NewMySQLReaderJob()
	err := task.readerJob.Init(config)
	if err != nil {
		return err
	}

	// 建立数据库连接
	task.db, err = task.readerJob.connect()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	return nil
}

func (task *MySQLReaderTask) StartRead(recordSender plugin.RecordSender) error {
	defer func() {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	// 构建查询SQL
	query, err := task.buildQuery()
	if err != nil {
		return err
	}

	log.Printf("Executing query: %s", query)

	// 执行查询
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

	// 创建扫描目标
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for idx := range values {
		valuePtrs[idx] = &values[idx]
	}

	recordCount := 0
	for rows.Next() {
		// 扫描行数据
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		// 创建记录
		record := element.NewRecord()
		for _, value := range values {
			column := task.convertToColumn(value)
			record.AddColumn(column)
		}

		// 发送记录
		if err := recordSender.SendRecord(record); err != nil {
			return fmt.Errorf("failed to send record: %v", err)
		}

		recordCount++
		if recordCount%1000 == 0 {
			log.Printf("Read %d records", recordCount)
		}
	}

	log.Printf("Total records read: %d", recordCount)
	return nil
}

func (task *MySQLReaderTask) buildQuery() (string, error) {
	// 如果配置了querySql，直接使用用户提供的SQL
	if task.readerJob.querySql != "" {
		return task.readerJob.querySql, nil
	}

	// 构建SELECT语句（table/column模式）
	columns := task.readerJob.columns
	table := task.readerJob.tables[0]

	var columnStr string
	if len(columns) == 1 && columns[0] == "*" {
		columnStr = "*"
	} else {
		// MySQL使用反引号包围列名和表名
		quotedColumns := make([]string, len(columns))
		for i, col := range columns {
			quotedColumns[i] = fmt.Sprintf("`%s`", col)
		}
		columnStr = strings.Join(quotedColumns, ", ")
	}

	query := fmt.Sprintf("SELECT %s FROM `%s`", columnStr, table)

	// 添加WHERE条件
	var whereConditions []string

	// 原始WHERE条件
	if task.readerJob.where != "" {
		whereConditions = append(whereConditions, task.readerJob.where)
	}

	// 分片范围条件
	if splitRange := task.config.Get("parameter.splitRange"); splitRange != nil {
		if rangeMap, ok := splitRange.(map[string]interface{}); ok {
			preWhere, postWhere, limitClause := task.buildSplitCondition(rangeMap)

			if preWhere != "" {
				whereConditions = append(whereConditions, preWhere)
			}

			if len(whereConditions) > 0 {
				query += " WHERE " + strings.Join(whereConditions, " AND ")
			}

			if postWhere != "" {
				query += " " + postWhere
			}

			if limitClause != "" {
				query += " " + limitClause
			}
		}
	} else {
		if len(whereConditions) > 0 {
			query += " WHERE " + strings.Join(whereConditions, " AND ")
		}
	}

	return query, nil
}

// buildSplitCondition 根据分片类型构建SQL条件
func (task *MySQLReaderTask) buildSplitCondition(rangeMap map[string]interface{}) (string, string, string) {
	splitType, _ := rangeMap["type"].(string)
	switch splitType {
	case "numeric":
		return task.buildNumericCondition(rangeMap)
	case "text_dictionary":
		return task.buildTextDictionaryCondition(rangeMap)
	case "offset":
		return task.buildOffsetCondition(rangeMap)
	case "hash":
		return task.buildHashCondition(rangeMap)
	default:
		return task.buildNumericCondition(rangeMap) // 向后兼容
	}
}

// buildNumericCondition 构建数值类型的分片条件
func (task *MySQLReaderTask) buildNumericCondition(rangeMap map[string]interface{}) (string, string, string) {
	start := rangeMap["start"]
	end := rangeMap["end"]
	condition := fmt.Sprintf("`%s` >= %v AND `%s` <= %v",
		task.readerJob.splitPk, start, task.readerJob.splitPk, end)
	return condition, "", ""
}

// buildTextDictionaryCondition 构建文本字典序的分片条件
func (task *MySQLReaderTask) buildTextDictionaryCondition(rangeMap map[string]interface{}) (string, string, string) {
	start := rangeMap["start"]
	end := rangeMap["end"]

	var condition string
	if start != nil && end != nil {
		condition = fmt.Sprintf("`%s` >= '%s' AND `%s` <= '%s'",
			task.readerJob.splitPk, start, task.readerJob.splitPk, end)
	} else if start != nil {
		condition = fmt.Sprintf("`%s` >= '%s'", task.readerJob.splitPk, start)
	} else if end != nil {
		condition = fmt.Sprintf("`%s` <= '%s'", task.readerJob.splitPk, end)
	}

	return condition, "", ""
}

// buildOffsetCondition 构建基于OFFSET的分片条件
func (task *MySQLReaderTask) buildOffsetCondition(rangeMap map[string]interface{}) (string, string, string) {
	offset := rangeMap["offset"]
	limit := rangeMap["limit"]

	orderBy := fmt.Sprintf("ORDER BY `%s`", task.readerJob.splitPk)
	limitClause := fmt.Sprintf("LIMIT %v OFFSET %v", limit, offset)

	return "", orderBy, limitClause
}

// buildHashCondition 构建基于hash的分片条件
func (task *MySQLReaderTask) buildHashCondition(rangeMap map[string]interface{}) (string, string, string) {
	taskId := rangeMap["taskId"]
	total := rangeMap["total"]

	// MySQL使用CRC32函数进行hash
	condition := fmt.Sprintf("CRC32(`%s`) %% %v = %v",
		task.readerJob.splitPk, total, taskId)

	return condition, "", ""
}

func (task *MySQLReaderTask) convertToColumn(value interface{}) element.Column {
	if value == nil {
		return element.NewStringColumn("")
	}

	switch v := value.(type) {
	case int64:
		return element.NewLongColumn(v)
	case int32:
		return element.NewLongColumn(int64(v))
	case int:
		return element.NewLongColumn(int64(v))
	case float64:
		return element.NewDoubleColumn(v)
	case float32:
		return element.NewDoubleColumn(float64(v))
	case string:
		return element.NewStringColumn(v)
	case []byte:
		return element.NewStringColumn(string(v))
	case time.Time:
		return element.NewDateColumn(v)
	case bool:
		return element.NewBoolColumn(v)
	default:
		// 其他类型转换为字符串
		return element.NewStringColumn(fmt.Sprintf("%v", v))
	}
}

func (task *MySQLReaderTask) Post() error {
	return nil
}

func (task *MySQLReaderTask) Destroy() error {
	if task.db != nil {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}
	return nil
}