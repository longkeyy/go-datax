package postgresqlreader

import (
	"database/sql"
	"fmt"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"log"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	DefaultFetchSize = 1024
)

// PostgreSQLReaderJob PostgreSQL读取作业
type PostgreSQLReaderJob struct {
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

func NewPostgreSQLReaderJob() *PostgreSQLReaderJob {
	return &PostgreSQLReaderJob{}
}

func (job *PostgreSQLReaderJob) Init(config *config.Configuration) error {
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
		log.Printf("PostgreSQL Reader initialized with querySql mode")
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
		log.Printf("PostgreSQL Reader initialized with table/column mode: tables=%v, columns=%v", job.tables, job.columns)
	}

	return nil
}

func (job *PostgreSQLReaderJob) Split(adviceNumber int) ([]*config.Configuration, error) {
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

func (job *PostgreSQLReaderJob) calculateSplitRanges(adviceNumber int) ([]map[string]interface{}, error) {
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
		return nil, fmt.Errorf("unsupported column type '%s' for splitPk. Supported types: integer, text, varchar", columnType)
	}
}

// detectColumnType 检测列的数据类型
func (job *PostgreSQLReaderJob) detectColumnType(db *gorm.DB, table, column string) (string, error) {
	query := `
		SELECT data_type
		FROM information_schema.columns
		WHERE table_name = ? AND column_name = ? AND table_schema = 'public'
	`

	var dataType string
	err := db.Raw(query, table, column).Row().Scan(&dataType)
	if err != nil {
		return "", fmt.Errorf("failed to query column type: %v", err)
	}

	return dataType, nil
}

// isNumericType 判断是否为数值类型
func (job *PostgreSQLReaderJob) isNumericType(dataType string) bool {
	numericTypes := []string{
		"integer", "int", "int4", "bigint", "int8", "smallint", "int2",
		"serial", "bigserial", "smallserial",
	}

	for _, t := range numericTypes {
		if strings.EqualFold(dataType, t) {
			return true
		}
	}
	return false
}

// isTextType 判断是否为文本类型
func (job *PostgreSQLReaderJob) isTextType(dataType string) bool {
	textTypes := []string{
		"text", "varchar", "character varying", "char", "character",
		"uuid", "bpchar",
	}

	for _, t := range textTypes {
		if strings.EqualFold(dataType, t) {
			return true
		}
	}
	return false
}

// calculateNumericSplitRanges 计算数值类型的分片范围（原有逻辑）
func (job *PostgreSQLReaderJob) calculateNumericSplitRanges(db *gorm.DB, adviceNumber int) ([]map[string]interface{}, error) {
	table := job.tables[0]
	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", job.splitPk, job.splitPk, table)
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

// calculateTextSplitRanges 计算文本类型的分片范围
func (job *PostgreSQLReaderJob) calculateTextSplitRanges(db *gorm.DB, adviceNumber int, columnType string) ([]map[string]interface{}, error) {
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

// calculateTextDictionarySplitRanges 基于字典序的文本切分
func (job *PostgreSQLReaderJob) calculateTextDictionarySplitRanges(db *gorm.DB, adviceNumber int) ([]map[string]interface{}, error) {
	table := job.tables[0]

	// 获取MIN和MAX值
	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", job.splitPk, job.splitPk, table)
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

	// 基于字典序生成范围
	ranges := make([]map[string]interface{}, 0)

	// 首先获取总记录数
	countQuery := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM %s
		%s
	`, table,
		func() string {
			if job.where != "" {
				return "WHERE " + job.where
			}
			return ""
		}())

	var totalCount int64
	if err := db.Raw(countQuery).Row().Scan(&totalCount); err != nil {
		return nil, fmt.Errorf("failed to get total count: %v", err)
	}

	// 对于大数据集或者需要更多分片时，优先使用OFFSET切分确保均匀分布
	if totalCount > int64(adviceNumber*2000) || adviceNumber > 4 {
		log.Printf("Using offset-based splitting for better data distribution (totalCount=%d, channels=%d)", totalCount, adviceNumber)
		return job.calculateOffsetSplitRanges(db, adviceNumber)
	}

	// 对于小数据集，使用字典序切分

	sampleQuery := fmt.Sprintf(`
		SELECT DISTINCT %s
		FROM %s
		%s
		ORDER BY %s
		LIMIT %d
	`, job.splitPk, table,
		func() string {
			if job.where != "" {
				return "WHERE " + job.where
			}
			return ""
		}(),
		job.splitPk, adviceNumber*3) // 采样更多值来选择边界

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

// calculateOffsetSplitRanges 基于OFFSET/LIMIT的切分
func (job *PostgreSQLReaderJob) calculateOffsetSplitRanges(db *gorm.DB, adviceNumber int) ([]map[string]interface{}, error) {
	table := job.tables[0]

	// 获取总行数
	var totalCount int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
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

// calculateTextSampleSplitRanges 基于采样的文本切分（已废弃，保留用于向后兼容）
func (job *PostgreSQLReaderJob) calculateTextSampleSplitRanges(db *gorm.DB, adviceNumber int) ([]map[string]interface{}, error) {
	table := job.tables[0]

	// 获取总行数（用于计算采样间隔）
	var totalCount int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
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

	// 如果数据量很小，直接使用hash分片
	if totalCount < int64(adviceNumber*1000) {
		return job.calculateHashSplitRanges(adviceNumber), nil
	}

	// 采样获取边界值
	sampleQuery := fmt.Sprintf(`
		SELECT %s
		FROM (
			SELECT %s, ROW_NUMBER() OVER (ORDER BY %s) as rn
			FROM %s
			%s
		) t
		WHERE rn %% %d = 0
		ORDER BY %s
		LIMIT %d
	`, job.splitPk, job.splitPk, job.splitPk, table,
		func() string {
			if job.where != "" {
				return "WHERE " + job.where
			}
			return ""
		}(),
		totalCount/int64(adviceNumber*10), // 每个分片采样约10个边界候选
		job.splitPk, adviceNumber-1)

	rows, err := db.Raw(sampleQuery).Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to sample boundary values: %v", err)
	}
	defer rows.Close()

	var boundaries []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, fmt.Errorf("failed to scan boundary value: %v", err)
		}
		boundaries = append(boundaries, value)
	}

	// 构建范围
	ranges := make([]map[string]interface{}, 0)

	for i := 0; i < adviceNumber; i++ {
		var start, end interface{}
		var startOp, endOp string

		if i == 0 {
			start = nil // 第一个分片没有下界
			startOp = ""
		} else {
			start = boundaries[i-1]
			startOp = ">="
		}

		if i == adviceNumber-1 {
			end = nil // 最后一个分片没有上界
			endOp = ""
		} else if i < len(boundaries) {
			end = boundaries[i]
			endOp = "<"
		} else {
			end = nil
			endOp = ""
		}

		ranges = append(ranges, map[string]interface{}{
			"start":    start,
			"end":      end,
			"startOp":  startOp,
			"endOp":    endOp,
			"type":     "text_sample",
			"taskId":   i,
		})
	}

	return ranges, nil
}

// calculateHashSplitRanges 基于hash的切分（兜底方案）
func (job *PostgreSQLReaderJob) calculateHashSplitRanges(adviceNumber int) []map[string]interface{} {
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

func (job *PostgreSQLReaderJob) connect() (*gorm.DB, error) {
	// 构建连接字符串
	jdbcUrl := job.jdbcUrls[0]

	// 转换JDBC URL为PostgreSQL连接字符串
	dsn, err := job.convertJdbcUrl(jdbcUrl)
	if err != nil {
		return nil, err
	}

	// 连接数据库
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	return db, nil
}

func (job *PostgreSQLReaderJob) convertJdbcUrl(jdbcUrl string) (string, error) {
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
		job.username,
		job.password,
		database)

	return dsn, nil
}

func (job *PostgreSQLReaderJob) Post() error {
	return nil
}

func (job *PostgreSQLReaderJob) Destroy() error {
	return nil
}

// PostgreSQLReaderTask PostgreSQL读取任务
type PostgreSQLReaderTask struct {
	config    *config.Configuration
	readerJob *PostgreSQLReaderJob
	db        *gorm.DB
}

func NewPostgreSQLReaderTask() *PostgreSQLReaderTask {
	return &PostgreSQLReaderTask{}
}

func (task *PostgreSQLReaderTask) Init(config *config.Configuration) error {
	task.config = config

	// 创建ReaderJob来重用连接逻辑
	task.readerJob = NewPostgreSQLReaderJob()
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

func (task *PostgreSQLReaderTask) StartRead(recordSender plugin.RecordSender) error {
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

func (task *PostgreSQLReaderTask) buildQuery() (string, error) {
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
		columnStr = strings.Join(columns, ", ")
	}

	query := fmt.Sprintf("SELECT %s FROM %s", columnStr, table)

	// 添加WHERE条件
	var whereConditions []string

	// 原始WHERE条件
	if task.readerJob.where != "" {
		whereConditions = append(whereConditions, task.readerJob.where)
	}

	// 分片范围条件
	if splitRange := task.config.Get("parameter.splitRange"); splitRange != nil {
		if rangeMap, ok := splitRange.(map[string]interface{}); ok {
			condition, orderBy, limitClause := task.buildSplitCondition(rangeMap)
			if condition != "" {
				whereConditions = append(whereConditions, condition)
			}

			// 添加WHERE条件
			if len(whereConditions) > 0 {
				query += " WHERE " + strings.Join(whereConditions, " AND ")
			}

			// 添加ORDER BY（对于某些分片类型需要）
			if orderBy != "" {
				query += " ORDER BY " + orderBy
			}

			// 添加LIMIT子句（用于OFFSET分片）
			if limitClause != "" {
				query += " " + limitClause
			}

			return query, nil
		}
	}

	if len(whereConditions) > 0 {
		query += " WHERE " + strings.Join(whereConditions, " AND ")
	}

	return query, nil
}

// buildSplitCondition 根据分片类型构建WHERE条件、ORDER BY和LIMIT子句
func (task *PostgreSQLReaderTask) buildSplitCondition(rangeMap map[string]interface{}) (string, string, string) {
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
		// 向后兼容：如果没有type字段，假设是numeric类型
		return task.buildNumericCondition(rangeMap)
	}
}

// buildNumericCondition 构建数值类型的分片条件
func (task *PostgreSQLReaderTask) buildNumericCondition(rangeMap map[string]interface{}) (string, string, string) {
	start := rangeMap["start"]
	end := rangeMap["end"]
	condition := fmt.Sprintf("%s >= %v AND %s <= %v",
		task.readerJob.splitPk, start, task.readerJob.splitPk, end)
	return condition, "", ""
}

// buildTextDictionaryCondition 构建文本字典序的分片条件
func (task *PostgreSQLReaderTask) buildTextDictionaryCondition(rangeMap map[string]interface{}) (string, string, string) {
	var conditions []string

	if start := rangeMap["start"]; start != nil {
		startOp := rangeMap["startOp"].(string)
		if startOp != "" {
			conditions = append(conditions, fmt.Sprintf("%s %s '%s'", task.readerJob.splitPk, startOp, start))
		}
	}

	if end := rangeMap["end"]; end != nil {
		endOp := rangeMap["endOp"].(string)
		if endOp != "" {
			conditions = append(conditions, fmt.Sprintf("%s %s '%s'", task.readerJob.splitPk, endOp, end))
		}
	}

	condition := strings.Join(conditions, " AND ")
	orderBy := task.readerJob.splitPk // 按splitPk排序保证稳定的结果
	return condition, orderBy, ""
}

// buildOffsetCondition 构建OFFSET/LIMIT的分片条件
func (task *PostgreSQLReaderTask) buildOffsetCondition(rangeMap map[string]interface{}) (string, string, string) {
	offset := rangeMap["offset"].(int64)
	limit := rangeMap["limit"].(int64)

	orderBy := task.readerJob.splitPk // 按splitPk排序保证稳定的分片
	limitClause := fmt.Sprintf("LIMIT %d OFFSET %d", limit, offset)

	return "", orderBy, limitClause
}

// buildHashCondition 构建Hash分片条件
func (task *PostgreSQLReaderTask) buildHashCondition(rangeMap map[string]interface{}) (string, string, string) {
	taskId := rangeMap["taskId"].(int)
	total := rangeMap["total"].(int)

	// 使用PostgreSQL的hashtext函数
	condition := fmt.Sprintf("MOD(ABS(HASHTEXT(%s)), %d) = %d", task.readerJob.splitPk, total, taskId)
	return condition, "", ""
}

func (task *PostgreSQLReaderTask) convertToColumn(value interface{}) element.Column {
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

func (task *PostgreSQLReaderTask) Post() error {
	return nil
}

func (task *PostgreSQLReaderTask) Destroy() error {
	if task.db != nil {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}
	return nil
}