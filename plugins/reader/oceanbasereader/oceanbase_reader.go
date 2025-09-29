package oceanbasereader

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
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
)

const (
	DefaultFetchSize           = 1024
	DefaultQueryTimeoutSeconds = 48 * 3600 // 48 hours
	OBSplitString             = "||_dsc_ob10_dsc_||"
	OBSplitStringPattern      = "\\|\\|_dsc_ob10_dsc_\\|\\|"
	OBCompatibleModeMySQL     = "MYSQL"
	OBCompatibleModeOracle    = "ORACLE"
)

// OceanBaseReaderJob OceanBase读取作业
type OceanBaseReaderJob struct {
	config            config.Configuration
	username          string
	password          string
	jdbcUrls          []string
	tables            []string
	columns           []string
	where             string
	splitPk           string
	querySql          string // 支持自定义查询SQL
	weakRead          bool   // 弱一致性读取
	compatibleMode    string // 兼容模式：MYSQL或ORACLE
	readByPartition   bool   // 是否按分区读取
	queryTimeoutSecs  int    // 查询超时时间（秒）
	factory           *factory.DataXFactory
}

func NewOceanBaseReaderJob() *OceanBaseReaderJob {
	return &OceanBaseReaderJob{
		factory:          factory.GetGlobalFactory(),
		compatibleMode:   OBCompatibleModeMySQL, // 默认MySQL模式
		weakRead:         true,                  // 默认使用弱一致性读取
		queryTimeoutSecs: DefaultQueryTimeoutSeconds,
	}
}

func (job *OceanBaseReaderJob) Init(config config.Configuration) error {
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

	// 处理OceanBase特有的JDBC URL格式
	if err := job.processOBJdbcUrls(); err != nil {
		return err
	}

	// 获取可选参数
	job.where = config.GetString("parameter.where")
	job.splitPk = config.GetString("parameter.splitPk")
	job.querySql = config.GetString("parameter.querySql")
	job.weakRead = config.GetBoolWithDefault("parameter.weakRead", true)
	job.readByPartition = config.GetBoolWithDefault("parameter.readByPartition", false)
	job.queryTimeoutSecs = config.GetIntWithDefault("parameter.queryTimeoutSeconds", DefaultQueryTimeoutSeconds)

	// 检查配置模式：querySql 或 table/column 模式
	if job.querySql != "" {
		// querySql模式：直接使用用户提供的SQL查询
		logger.Component().WithComponent("OceanBaseReader").Info("OceanBase Reader initialized with querySql mode")
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

		// 转义关键字（OceanBase特有）
		job.escapeKeywords()

		logger.Component().WithComponent("OceanBaseReader").Info("OceanBase Reader initialized with table/column mode",
			zap.Any("tables", job.tables),
			zap.Any("columns", job.columns),
			zap.Bool("weakRead", job.weakRead),
			zap.String("compatibleMode", job.compatibleMode))
	}

	// 检测兼容模式
	if err := job.detectCompatibleMode(); err != nil {
		logger.Component().WithComponent("OceanBaseReader").Warn("Failed to detect compatible mode, using default MySQL mode",
			zap.Error(err))
	}

	return nil
}

// processOBJdbcUrls 处理OceanBase特有的JDBC URL格式
func (job *OceanBaseReaderJob) processOBJdbcUrls() error {
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

			logger.Component().WithComponent("OceanBaseReader").Info("Processed OceanBase 1.0 JDBC URL",
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

// escapeKeywords 转义OceanBase关键字
func (job *OceanBaseReaderJob) escapeKeywords() {
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

// detectCompatibleMode 检测OceanBase兼容模式
func (job *OceanBaseReaderJob) detectCompatibleMode() error {
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

	logger.Component().WithComponent("OceanBaseReader").Info("Detected OceanBase compatible mode",
		zap.String("compatibleMode", job.compatibleMode))
	return nil
}

func (job *OceanBaseReaderJob) Split(adviceNumber int) ([]config.Configuration, error) {
	taskConfigs := make([]config.Configuration, 0)

	// 如果使用querySql模式，则不进行分片，使用单个任务
	if job.querySql != "" {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", 0)
		taskConfigs = append(taskConfigs, taskConfig)
		logger.Component().WithComponent("OceanBaseReader").Info("Using querySql mode, single task")
		return taskConfigs, nil
	}

	// 检查是否使用分区读取模式
	if job.readByPartition && (job.splitPk == "" || strings.TrimSpace(job.splitPk) == "") {
		// 尝试按分区进行分片
		partitionConfigs, err := job.splitByPartition()
		if err != nil {
			logger.Component().WithComponent("OceanBaseReader").Warn("Failed to split by partition, fallback to splitPk mode",
				zap.Error(err))
		} else if len(partitionConfigs) > 0 {
			logger.Component().WithComponent("OceanBaseReader").Info("Split by partition",
				zap.Int("taskCount", len(partitionConfigs)))
			return partitionConfigs, nil
		}
	}

	// 如果没有设置splitPk，使用单个任务
	if job.splitPk == "" || strings.TrimSpace(job.splitPk) == "" {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", 0)
		taskConfigs = append(taskConfigs, taskConfig)
		logger.Component().WithComponent("OceanBaseReader").Info("No splitPk specified, using single task")
		return taskConfigs, nil
	}

	// 如果设置了splitPk，尝试进行分片
	ranges, err := job.calculateSplitRanges(adviceNumber)
	if err != nil {
		logger.Component().WithComponent("OceanBaseReader").Warn("Failed to calculate split ranges, fallback to single task",
			zap.Error(err))
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", 0)
		taskConfigs = append(taskConfigs, taskConfig)
		return taskConfigs, nil
	}

	// 创建分片任务配置，为每个任务添加负载均衡标记
	for i, splitRange := range ranges {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfig.Set("parameter.splitRange", splitRange)

		// 添加OceanBase负载均衡标记
		if obRegionName := job.getObRegionName(job.jdbcUrls[0]); obRegionName != "" {
			taskConfig.Set("loadBalanceResourceMark", obRegionName)
		}

		taskConfigs = append(taskConfigs, taskConfig)
	}

	logger.Component().WithComponent("OceanBaseReader").Info("Split into tasks based on splitPk",
		zap.Int("taskCount", len(taskConfigs)),
		zap.String("splitPk", job.splitPk))
	return taskConfigs, nil
}

// splitByPartition 按分区分片（OceanBase特有功能）
func (job *OceanBaseReaderJob) splitByPartition() ([]config.Configuration, error) {
	db, err := job.connect()
	if err != nil {
		return nil, err
	}
	defer func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	}()

	// 查询表的分区信息
	partitionQuery := `
		SELECT PARTITION_NAME
		FROM INFORMATION_SCHEMA.PARTITIONS
		WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE()
		AND PARTITION_NAME IS NOT NULL
		ORDER BY PARTITION_ORDINAL_POSITION`

	rows, err := db.Raw(partitionQuery, strings.Trim(job.tables[0], "`\"")).Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to query partitions: %v", err)
	}
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var partitionName string
		if err := rows.Scan(&partitionName); err != nil {
			continue
		}
		partitions = append(partitions, partitionName)
	}

	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partitions found for table %s", job.tables[0])
	}

	// 为每个分区创建任务配置
	taskConfigs := make([]config.Configuration, 0, len(partitions))
	for i, partition := range partitions {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfig.Set("parameter.partitionName", partition)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	return taskConfigs, nil
}

// getObRegionName 获取OceanBase Region名称用于负载均衡
func (job *OceanBaseReaderJob) getObRegionName(jdbcUrl string) string {
	if strings.HasPrefix(jdbcUrl, OBSplitString) {
		parts := strings.Split(jdbcUrl, OBSplitString)
		if len(parts) >= 2 {
			tenant := strings.TrimSpace(parts[1])
			if colonIdx := strings.Index(tenant, ":"); colonIdx != -1 {
				return tenant[:colonIdx]
			}
			return tenant
		}
	}
	return ""
}

func (job *OceanBaseReaderJob) calculateSplitRanges(adviceNumber int) ([]map[string]interface{}, error) {
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

// detectColumnType 检测列的数据类型（OceanBase版本）
func (job *OceanBaseReaderJob) detectColumnType(db *gorm.DB, table, column string) (string, error) {
	query := `
		SELECT DATA_TYPE
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME = ? AND COLUMN_NAME = ? AND TABLE_SCHEMA = DATABASE()
	`

	var dataType string
	err := db.Raw(query, strings.Trim(table, "`\""), strings.Trim(column, "`\"")).Row().Scan(&dataType)
	if err != nil {
		return "", fmt.Errorf("failed to query column type: %v", err)
	}

	return dataType, nil
}

// isNumericType 判断是否为数值类型（OceanBase版本）
func (job *OceanBaseReaderJob) isNumericType(dataType string) bool {
	numericTypes := []string{
		"int", "integer", "bigint", "smallint", "tinyint", "mediumint",
		"decimal", "numeric", "float", "double", "bit",
		// OceanBase Oracle模式的数值类型
		"number",
	}

	for _, t := range numericTypes {
		if strings.EqualFold(dataType, t) {
			return true
		}
	}
	return false
}

// isTextType 判断是否为文本类型（OceanBase版本）
func (job *OceanBaseReaderJob) isTextType(dataType string) bool {
	textTypes := []string{
		"varchar", "char", "text", "tinytext", "mediumtext", "longtext",
		"binary", "varbinary", "enum", "set",
		// OceanBase Oracle模式的字符类型
		"varchar2", "nvarchar2", "clob", "nclob",
	}

	for _, t := range textTypes {
		if strings.EqualFold(dataType, t) {
			return true
		}
	}
	return false
}

// calculateNumericSplitRanges 计算数值类型的分片范围（OceanBase版本）
func (job *OceanBaseReaderJob) calculateNumericSplitRanges(db *gorm.DB, adviceNumber int) ([]map[string]interface{}, error) {
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

// calculateTextSplitRanges 计算文本类型的分片范围（OceanBase版本）
func (job *OceanBaseReaderJob) calculateTextSplitRanges(db *gorm.DB, adviceNumber int, columnType string) ([]map[string]interface{}, error) {
	// 策略1: 尝试字典序范围切分
	ranges, err := job.calculateTextDictionarySplitRanges(db, adviceNumber)
	if err != nil {
		logger.Component().WithComponent("OceanBaseReader").Warn("Dictionary-based splitting failed, trying offset-based splitting",
			zap.Error(err))

		// 策略2: 使用OFFSET/LIMIT切分
		ranges, err = job.calculateOffsetSplitRanges(db, adviceNumber)
		if err != nil {
			logger.Component().WithComponent("OceanBaseReader").Warn("Offset-based splitting failed, falling back to hash-based splitting",
				zap.Error(err))
			// 策略3: hash切分（兜底）
			return job.calculateHashSplitRanges(adviceNumber), nil
		}
	}

	return ranges, nil
}

// calculateTextDictionarySplitRanges 基于字典序的文本切分（OceanBase版本）
func (job *OceanBaseReaderJob) calculateTextDictionarySplitRanges(db *gorm.DB, adviceNumber int) ([]map[string]interface{}, error) {
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

	// 采样获取边界值
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
		job.splitPk, adviceNumber*10) // 采样更多值来选择边界

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

// calculateOffsetSplitRanges 基于OFFSET/LIMIT的切分（OceanBase版本）
func (job *OceanBaseReaderJob) calculateOffsetSplitRanges(db *gorm.DB, adviceNumber int) ([]map[string]interface{}, error) {
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

// calculateHashSplitRanges 基于hash的切分（OceanBase版本）
func (job *OceanBaseReaderJob) calculateHashSplitRanges(adviceNumber int) []map[string]interface{} {
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

func (job *OceanBaseReaderJob) connect() (*gorm.DB, error) {
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

	// 设置连接超时和socket超时
	if sqlDB, err := db.DB(); err == nil {
		sqlDB.SetConnMaxLifetime(time.Duration(job.queryTimeoutSecs) * time.Second)
	}

	return db, nil
}

func (job *OceanBaseReaderJob) convertJdbcUrl(jdbcUrl string) (string, error) {
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

	// 构建MySQL DSN字符串，添加OceanBase优化参数
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&loc=Local&socketTimeout=%ds&connectTimeout=60s",
		job.username,
		job.password,
		hostPort,
		database,
		1800) // 30分钟socket超时

	// 添加额外参数
	if params != "" {
		dsn += "&" + params
	}

	return dsn, nil
}

func (job *OceanBaseReaderJob) Post() error {
	return nil
}

func (job *OceanBaseReaderJob) Destroy() error {
	return nil
}

// OceanBaseReaderTask OceanBase读取任务
type OceanBaseReaderTask struct {
	config       config.Configuration
	readerJob    *OceanBaseReaderJob
	db           *gorm.DB
	partitionName string // 分区名（用于分区读取）
	factory      *factory.DataXFactory
}

func NewOceanBaseReaderTask() *OceanBaseReaderTask {
	return &OceanBaseReaderTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *OceanBaseReaderTask) Init(config config.Configuration) error {
	task.config = config

	// 创建ReaderJob来重用连接逻辑
	task.readerJob = NewOceanBaseReaderJob()
	err := task.readerJob.Init(config)
	if err != nil {
		return err
	}

	// 建立数据库连接
	task.db, err = task.readerJob.connect()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	// 获取分区名（如果按分区读取）
	task.partitionName = config.GetString("parameter.partitionName")

	return nil
}

func (task *OceanBaseReaderTask) StartRead(recordSender plugin.RecordSender) error {
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

	logger.Component().WithComponent("OceanBaseReader").Info("Executing query",
		zap.String("query", query))

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
		record := task.factory.GetRecordFactory().CreateRecord()
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
			logger.Component().WithComponent("OceanBaseReader").Info("Reading progress",
				zap.Int("recordCount", recordCount))
		}
	}

	logger.Component().WithComponent("OceanBaseReader").Info("Reading completed",
		zap.Int("totalRecords", recordCount))
	return nil
}

func (task *OceanBaseReaderTask) buildQuery() (string, error) {
	// 如果配置了querySql，直接使用用户提供的SQL
	if task.readerJob.querySql != "" {
		query := task.readerJob.querySql
		// 如果启用弱一致性读取且SQL中没有hint，添加弱一致性读取hint
		if task.readerJob.weakRead && !strings.Contains(strings.ToLower(query), "read_consistency") {
			query = strings.Replace(query, "select", "select /*+read_consistency(weak)*/", 1)
		}
		return query, nil
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

	// 构建基础查询，根据兼容模式选择合适的语法
	var query string
	if task.readerJob.weakRead {
		// 添加弱一致性读取hint（OceanBase特有）
		query = fmt.Sprintf("SELECT /*+read_consistency(weak)*/ %s FROM %s", columnStr, table)
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s", columnStr, table)
	}

	// 如果是分区读取，添加分区条件
	if task.partitionName != "" {
		query += fmt.Sprintf(" PARTITION (%s)", task.partitionName)
	}

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
func (task *OceanBaseReaderTask) buildSplitCondition(rangeMap map[string]interface{}) (string, string, string) {
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
func (task *OceanBaseReaderTask) buildNumericCondition(rangeMap map[string]interface{}) (string, string, string) {
	start := rangeMap["start"]
	end := rangeMap["end"]
	condition := fmt.Sprintf("%s >= %v AND %s <= %v",
		task.readerJob.splitPk, start, task.readerJob.splitPk, end)
	return condition, "", ""
}

// buildTextDictionaryCondition 构建文本字典序的分片条件
func (task *OceanBaseReaderTask) buildTextDictionaryCondition(rangeMap map[string]interface{}) (string, string, string) {
	start := rangeMap["start"]
	end := rangeMap["end"]

	var condition string
	if start != nil && end != nil {
		condition = fmt.Sprintf("%s >= '%s' AND %s <= '%s'",
			task.readerJob.splitPk, start, task.readerJob.splitPk, end)
	} else if start != nil {
		condition = fmt.Sprintf("%s >= '%s'", task.readerJob.splitPk, start)
	} else if end != nil {
		condition = fmt.Sprintf("%s <= '%s'", task.readerJob.splitPk, end)
	}

	return condition, "", ""
}

// buildOffsetCondition 构建基于OFFSET的分片条件
func (task *OceanBaseReaderTask) buildOffsetCondition(rangeMap map[string]interface{}) (string, string, string) {
	offset := rangeMap["offset"]
	limit := rangeMap["limit"]

	orderBy := fmt.Sprintf("ORDER BY %s", task.readerJob.splitPk)
	limitClause := fmt.Sprintf("LIMIT %v OFFSET %v", limit, offset)

	return "", orderBy, limitClause
}

// buildHashCondition 构建基于hash的分片条件
func (task *OceanBaseReaderTask) buildHashCondition(rangeMap map[string]interface{}) (string, string, string) {
	taskId := rangeMap["taskId"]
	total := rangeMap["total"]

	// OceanBase使用CRC32函数进行hash（与MySQL兼容）
	condition := fmt.Sprintf("CRC32(%s) %% %v = %v",
		task.readerJob.splitPk, total, taskId)

	return condition, "", ""
}

func (task *OceanBaseReaderTask) convertToColumn(value interface{}) element.Column {
	columnFactory := task.factory.GetColumnFactory()
	if value == nil {
		return columnFactory.CreateStringColumn("")
	}

	switch v := value.(type) {
	case int64:
		return columnFactory.CreateLongColumn(v)
	case int32:
		return columnFactory.CreateLongColumn(int64(v))
	case int:
		return columnFactory.CreateLongColumn(int64(v))
	case float64:
		return columnFactory.CreateDoubleColumn(v)
	case float32:
		return columnFactory.CreateDoubleColumn(float64(v))
	case string:
		return columnFactory.CreateStringColumn(v)
	case []byte:
		return columnFactory.CreateStringColumn(string(v))
	case time.Time:
		return columnFactory.CreateDateColumn(v)
	case bool:
		return columnFactory.CreateBoolColumn(v)
	default:
		// 其他类型转换为字符串
		return columnFactory.CreateStringColumn(fmt.Sprintf("%v", v))
	}
}

func (task *OceanBaseReaderTask) Post() error {
	return nil
}

func (task *OceanBaseReaderTask) Destroy() error {
	if task.db != nil {
		if sqlDB, err := task.db.DB(); err == nil {
			sqlDB.Close()
		}
	}
	return nil
}