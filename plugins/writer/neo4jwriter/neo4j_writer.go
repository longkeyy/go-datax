package neo4jwriter

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/factory"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/plugin"
	coreplugin "github.com/longkeyy/go-datax/core/registry"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"go.uber.org/zap"
)

const (
	DefaultBatchSize                    = 1000
	DefaultMaxTransactionRetryTime      = 30
	DefaultMaxConnectionTimeout         = 30
	DefaultRetryTimes                  = 3
	DefaultRetrySleepMills             = 3000
	DefaultBatchDataVariableName        = "batch"
)

// Neo4jProperty 表示Neo4j属性配置，对应Java版本的配置结构
type Neo4jProperty struct {
	Name       string `json:"name"`       // Neo4j字段名
	Type       string `json:"type"`       // Neo4j数据类型
	DateFormat string `json:"dateFormat"` // 日期格式
	Split      string `json:"split"`      // 数组分隔符
}

// GetSplitOrDefault 获取分割符或默认值
func (p *Neo4jProperty) GetSplitOrDefault() string {
	if p.Split == "" {
		return ","
	}
	return p.Split
}

// Neo4jWriterJob Neo4j写入作业
type Neo4jWriterJob struct {
	config   config.Configuration
	factory  *factory.DataXFactory

	// 数据库连接配置
	uri         string
	username    string
	password    string
	bearerToken string
	kerberosTicket string
	database    string

	// 写入配置
	cypher               string
	batchDataVariableName string
	properties           []*Neo4jProperty
	batchSize            int

	// 连接配置
	maxTransactionRetryTimeSeconds int
	maxConnectionTimeoutSeconds    int
	retryTimes                    int
	retrySleepMills               int
}

func NewNeo4jWriterJob() *Neo4jWriterJob {
	return &Neo4jWriterJob{
		factory: factory.GetGlobalFactory(),
	}
}

func (job *Neo4jWriterJob) Init(config config.Configuration) error {
	job.config = config

	// 获取数据库连接参数
	job.uri = config.GetString("parameter.uri")
	if job.uri == "" {
		return fmt.Errorf("uri is required")
	}

	job.username = config.GetString("parameter.username")
	job.password = config.GetString("parameter.password")
	job.bearerToken = config.GetString("parameter.bearerToken")
	job.kerberosTicket = config.GetString("parameter.kerberosTicket")
	job.database = config.GetString("parameter.database")

	// 验证认证配置
	hasBasicAuth := job.username != "" && job.password != ""
	hasBearerAuth := job.bearerToken != ""
	hasKerberosAuth := job.kerberosTicket != ""

	if !hasBasicAuth && !hasBearerAuth && !hasKerberosAuth {
		return fmt.Errorf("authentication configuration is required: username/password, bearerToken, or kerberosTicket")
	}

	// 获取Cypher语句
	job.cypher = config.GetString("parameter.cypher")
	if job.cypher == "" {
		return fmt.Errorf("cypher is required")
	}

	// 获取批处理变量名
	job.batchDataVariableName = config.GetString("parameter.batchDataVariableName")
	if job.batchDataVariableName == "" {
		job.batchDataVariableName = DefaultBatchDataVariableName
	}

	// 解析properties配置
	propertiesStr := config.GetString("parameter.properties")
	if propertiesStr == "" {
		return fmt.Errorf("properties configuration is required")
	}

	if err := json.Unmarshal([]byte(propertiesStr), &job.properties); err != nil {
		return fmt.Errorf("failed to parse properties configuration: %v", err)
	}

	if len(job.properties) == 0 {
		return fmt.Errorf("at least one property must be configured")
	}

	// 获取可选参数
	job.batchSize = config.GetInt("parameter.batchSize")
	if job.batchSize <= 0 {
		job.batchSize = DefaultBatchSize
	}

	job.maxTransactionRetryTimeSeconds = config.GetInt("parameter.maxTransactionRetryTimeSeconds")
	if job.maxTransactionRetryTimeSeconds <= 0 {
		job.maxTransactionRetryTimeSeconds = DefaultMaxTransactionRetryTime
	}

	job.maxConnectionTimeoutSeconds = config.GetInt("parameter.maxConnectionTimeoutSeconds")
	if job.maxConnectionTimeoutSeconds <= 0 {
		job.maxConnectionTimeoutSeconds = DefaultMaxConnectionTimeout
	}

	job.retryTimes = config.GetInt("parameter.retryTimes")
	if job.retryTimes <= 0 {
		job.retryTimes = DefaultRetryTimes
	}

	job.retrySleepMills = config.GetInt("parameter.retrySleepMills")
	if job.retrySleepMills <= 0 {
		job.retrySleepMills = DefaultRetrySleepMills
	}

	logger.Component().WithComponent("Neo4jWriter").Info("Neo4j Writer initialized",
		zap.String("uri", job.uri),
		zap.String("database", job.database),
		zap.String("cypher", job.cypher),
		zap.String("batchDataVariableName", job.batchDataVariableName),
		zap.Int("batchSize", job.batchSize),
		zap.Int("propertiesCount", len(job.properties)))

	return nil
}

func (job *Neo4jWriterJob) Prepare() error {
	// Neo4j Writer通常不需要预处理
	return nil
}

func (job *Neo4jWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	taskConfigs := make([]config.Configuration, 0)

	// Neo4j Writer通常不需要分片，每个task处理不同的数据
	for i := 0; i < mandatoryNumber; i++ {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	logger.Component().WithComponent("Neo4jWriter").Info("Split into Neo4j writer tasks",
		zap.Int("taskCount", len(taskConfigs)))
	return taskConfigs, nil
}

func (job *Neo4jWriterJob) Post() error {
	// Neo4j Writer通常不需要后处理
	return nil
}

func (job *Neo4jWriterJob) Destroy() error {
	return nil
}

// Neo4jWriterTask Neo4j写入任务
type Neo4jWriterTask struct {
	config    config.Configuration
	writerJob *Neo4jWriterJob
	driver    neo4j.DriverWithContext
	session   neo4j.SessionWithContext
	records   []map[string]interface{}
	factory   *factory.DataXFactory
}

func NewNeo4jWriterTask() *Neo4jWriterTask {
	return &Neo4jWriterTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *Neo4jWriterTask) Init(config config.Configuration) error {
	task.config = config

	// 创建WriterJob来重用连接逻辑
	task.writerJob = NewNeo4jWriterJob()
	err := task.writerJob.Init(config)
	if err != nil {
		return err
	}

	// 建立Neo4j连接
	task.driver, err = task.createDriver()
	if err != nil {
		return fmt.Errorf("failed to create Neo4j driver: %v", err)
	}

	// 创建session
	ctx := context.Background()
	if task.writerJob.database != "" {
		task.session = task.driver.NewSession(ctx, neo4j.SessionConfig{
			DatabaseName: task.writerJob.database,
		})
	} else {
		task.session = task.driver.NewSession(ctx, neo4j.SessionConfig{})
	}

	// 初始化记录缓冲区
	task.records = make([]map[string]interface{}, 0, task.writerJob.batchSize)

	return nil
}

func (task *Neo4jWriterTask) createDriver() (neo4j.DriverWithContext, error) {
	auth := neo4j.NoAuth()

	// 配置认证
	if task.writerJob.username != "" && task.writerJob.password != "" {
		auth = neo4j.BasicAuth(task.writerJob.username, task.writerJob.password, "")
	} else if task.writerJob.bearerToken != "" {
		auth = neo4j.BearerAuth(task.writerJob.bearerToken)
	} else if task.writerJob.kerberosTicket != "" {
		auth = neo4j.KerberosAuth(task.writerJob.kerberosTicket)
	}

	// 配置连接参数
	config := func(config *neo4j.Config) {
		config.MaxConnectionPoolSize = 1
		config.ConnectionAcquisitionTimeout = time.Duration(task.writerJob.maxConnectionTimeoutSeconds*2) * time.Second
		config.SocketConnectTimeout = time.Duration(task.writerJob.maxConnectionTimeoutSeconds) * time.Second
		config.MaxTransactionRetryTime = time.Duration(task.writerJob.maxTransactionRetryTimeSeconds) * time.Second
	}

	driver, err := neo4j.NewDriverWithContext(task.writerJob.uri, auth, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create driver: %v", err)
	}

	// 验证连接
	ctx := context.Background()
	if err := driver.VerifyConnectivity(ctx); err != nil {
		driver.Close(ctx)
		return nil, fmt.Errorf("failed to verify connectivity: %v", err)
	}

	return driver, nil
}

func (task *Neo4jWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	defer func() {
		ctx := context.Background()
		if task.session != nil {
			task.session.Close(ctx)
		}
		if task.driver != nil {
			task.driver.Close(ctx)
		}
	}()

	recordCount := 0
	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == coreplugin.ErrChannelClosed {
				break
			}
			return fmt.Errorf("failed to get record: %v", err)
		}

		// 转换记录并添加到缓冲区
		neo4jRecord, err := task.convertRecord(record)
		if err != nil {
			logger.Component().WithComponent("Neo4jWriter").Warn("Failed to convert record, skipping",
				zap.Error(err))
			continue
		}

		task.records = append(task.records, neo4jRecord)

		// 检查是否需要批量写入
		if len(task.records) >= task.writerJob.batchSize {
			if err := task.flushRecords(); err != nil {
				return err
			}
			recordCount += len(task.records)
			task.records = task.records[:0] // 清空缓冲区
		}
	}

	// 写入剩余记录
	if len(task.records) > 0 {
		if err := task.flushRecords(); err != nil {
			return err
		}
		recordCount += len(task.records)
	}

	logger.Component().WithComponent("Neo4jWriter").Info("Writing completed",
		zap.Int("totalRecords", recordCount))
	return nil
}

func (task *Neo4jWriterTask) convertRecord(record element.Record) (map[string]interface{}, error) {
	sourceColNum := record.GetColumnNumber()
	properties := task.writerJob.properties

	if len(properties) != sourceColNum {
		return nil, fmt.Errorf("the read and write columns do not match! expected %d columns, got %d", len(properties), sourceColNum)
	}

	data := make(map[string]interface{}, len(properties))
	for i := 0; i < sourceColNum; i++ {
		column := record.GetColumn(i)
		property := properties[i]

		value, err := task.convertColumnValue(column, property)
		if err != nil {
			return nil, fmt.Errorf("failed to convert column %d (%s): %v", i, property.Name, err)
		}

		data[property.Name] = value
	}

	return data, nil
}

func (task *Neo4jWriterTask) convertColumnValue(column element.Column, property *Neo4jProperty) (interface{}, error) {
	if column == nil {
		return nil, nil
	}

	typeStr := strings.ToUpper(property.Type)

	switch typeStr {
	case "BOOLEAN":
		return column.GetAsBool()
	case "STRING":
		return column.GetAsString(), nil
	case "LONG":
		return column.GetAsLong()
	case "SHORT":
		longVal, err := column.GetAsLong()
		if err != nil {
			return nil, err
		}
		return int16(longVal), nil
	case "INTEGER", "INT":
		longVal, err := column.GetAsLong()
		if err != nil {
			return nil, err
		}
		return int(longVal), nil
	case "DOUBLE":
		return column.GetAsDouble()
	case "FLOAT":
		doubleVal, err := column.GetAsDouble()
		if err != nil {
			return nil, err
		}
		return float32(doubleVal), nil
	case "LOCAL_DATE":
		dateVal, err := column.GetAsDate()
		if err != nil {
			return nil, err
		}
		return dateVal, nil
	case "LOCAL_TIME":
		// Neo4j LocalTime，这里简化处理，使用字符串
		return column.GetAsString(), nil
	case "LOCAL_DATE_TIME":
		dateVal, err := column.GetAsDate()
		if err != nil {
			return nil, err
		}
		return dateVal, nil
	case "LIST":
		// 处理列表类型，从字符串分割
		str := column.GetAsString()
		if str == "" {
			return []interface{}{}, nil
		}
		split := property.GetSplitOrDefault()
		parts := strings.Split(str, split)
		result := make([]interface{}, len(parts))
		for i, part := range parts {
			result[i] = strings.TrimSpace(part)
		}
		return result, nil
	case "MAP":
		// 处理Map类型，假设是JSON字符串
		str := column.GetAsString()
		if str == "" {
			return map[string]interface{}{}, nil
		}
		var result map[string]interface{}
		if err := json.Unmarshal([]byte(str), &result); err != nil {
			return nil, fmt.Errorf("failed to parse MAP value: %v", err)
		}
		return result, nil
	case "CHAR_ARRAY":
		str := column.GetAsString()
		return []rune(str), nil
	case "BYTE_ARRAY":
		return column.GetAsBytes()
	case "BOOLEAN_ARRAY":
		return task.parseArray(column.GetAsString(), property.GetSplitOrDefault(), "boolean")
	case "STRING_ARRAY":
		return task.parseArray(column.GetAsString(), property.GetSplitOrDefault(), "string")
	case "LONG_ARRAY":
		return task.parseArray(column.GetAsString(), property.GetSplitOrDefault(), "long")
	case "INT_ARRAY":
		return task.parseArray(column.GetAsString(), property.GetSplitOrDefault(), "int")
	case "SHORT_ARRAY":
		return task.parseArray(column.GetAsString(), property.GetSplitOrDefault(), "short")
	case "DOUBLE_ARRAY":
		return task.parseArray(column.GetAsString(), property.GetSplitOrDefault(), "double")
	case "FLOAT_ARRAY":
		return task.parseArray(column.GetAsString(), property.GetSplitOrDefault(), "float")
	case "OBJECT_ARRAY":
		return task.parseArray(column.GetAsString(), property.GetSplitOrDefault(), "object")
	default:
		// 默认作为字符串处理
		return column.GetAsString(), nil
	}
}

func (task *Neo4jWriterTask) parseArray(str, split, elementType string) (interface{}, error) {
	if str == "" {
		return []interface{}{}, nil
	}

	parts := strings.Split(str, split)
	switch elementType {
	case "boolean":
		result := make([]bool, len(parts))
		for i, part := range parts {
			val, err := strconv.ParseBool(strings.TrimSpace(part))
			if err != nil {
				return nil, fmt.Errorf("failed to parse boolean array element '%s': %v", part, err)
			}
			result[i] = val
		}
		return result, nil
	case "string":
		result := make([]string, len(parts))
		for i, part := range parts {
			result[i] = strings.TrimSpace(part)
		}
		return result, nil
	case "long":
		result := make([]int64, len(parts))
		for i, part := range parts {
			val, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse long array element '%s': %v", part, err)
			}
			result[i] = val
		}
		return result, nil
	case "int":
		result := make([]int, len(parts))
		for i, part := range parts {
			val, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil {
				return nil, fmt.Errorf("failed to parse int array element '%s': %v", part, err)
			}
			result[i] = val
		}
		return result, nil
	case "short":
		result := make([]int16, len(parts))
		for i, part := range parts {
			val, err := strconv.ParseInt(strings.TrimSpace(part), 10, 16)
			if err != nil {
				return nil, fmt.Errorf("failed to parse short array element '%s': %v", part, err)
			}
			result[i] = int16(val)
		}
		return result, nil
	case "double":
		result := make([]float64, len(parts))
		for i, part := range parts {
			val, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse double array element '%s': %v", part, err)
			}
			result[i] = val
		}
		return result, nil
	case "float":
		result := make([]float32, len(parts))
		for i, part := range parts {
			val, err := strconv.ParseFloat(strings.TrimSpace(part), 32)
			if err != nil {
				return nil, fmt.Errorf("failed to parse float array element '%s': %v", part, err)
			}
			result[i] = float32(val)
		}
		return result, nil
	case "object":
		result := make([]interface{}, len(parts))
		for i, part := range parts {
			result[i] = strings.TrimSpace(part)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported array element type: %s", elementType)
	}
}

func (task *Neo4jWriterTask) flushRecords() error {
	if len(task.records) == 0 {
		return nil
	}

	ctx := context.Background()
	parameters := map[string]interface{}{
		task.writerJob.batchDataVariableName: task.records,
	}

	// 执行Cypher语句，使用重试机制
	return task.executeWithRetry(ctx, task.writerJob.cypher, parameters)
}

func (task *Neo4jWriterTask) executeWithRetry(ctx context.Context, cypher string, parameters map[string]interface{}) error {
	var lastError error

	for attempt := 0; attempt <= task.writerJob.retryTimes; attempt++ {
		if attempt > 0 {
			// 等待重试间隔
			time.Sleep(time.Duration(task.writerJob.retrySleepMills) * time.Millisecond)
			logger.Component().WithComponent("Neo4jWriter").Warn("Retrying Neo4j write operation",
				zap.Int("attempt", attempt),
				zap.Int("maxRetries", task.writerJob.retryTimes))
		}

		// 执行写事务
		_, err := task.session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
			result, err := tx.Run(ctx, cypher, parameters)
			if err != nil {
				return nil, err
			}
			// 消费所有结果以确保语句执行完成
			_, err = result.Consume(ctx)
			return nil, err
		})

		if err == nil {
			return nil // 成功
		}

		lastError = err
		logger.Component().WithComponent("Neo4jWriter").Warn("Neo4j write operation failed",
			zap.Error(err),
			zap.Int("attempt", attempt+1),
			zap.String("cypher", cypher))
	}

	return fmt.Errorf("failed to execute Neo4j write after %d retries: %v", task.writerJob.retryTimes+1, lastError)
}

func (task *Neo4jWriterTask) Prepare() error {
	return nil
}

func (task *Neo4jWriterTask) Post() error {
	return nil
}

func (task *Neo4jWriterTask) Destroy() error {
	ctx := context.Background()
	if task.session != nil {
		task.session.Close(ctx)
	}
	if task.driver != nil {
		task.driver.Close(ctx)
	}
	return nil
}