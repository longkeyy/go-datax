package cassandrawriter

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/factory"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/plugin"
	"go.uber.org/zap"
)

const (
	DefaultPort                = 9042
	DefaultConsistency         = "LOCAL_QUORUM"
	DefaultConnectTimeout      = 5 * time.Second
	DefaultSocketTimeout       = 10 * time.Second
	DefaultBatchSize           = 1
	DefaultConnectionsPerHost  = 8
	DefaultMaxPendingPerConn   = 128
	WritetimeColumn           = "writetime()"
)

// Configuration keys matching Java implementation exactly
const (
	KeyUsername              = "username"
	KeyPassword              = "password"
	KeyHost                  = "host"
	KeyPort                  = "port"
	KeyUseSSL                = "useSSL"
	KeyKeyspace              = "keyspace"
	KeyTable                 = "table"
	KeyColumn                = "column"
	KeyConsistencyLevel      = "consistancyLevel"
	KeyBatchSize             = "batchSize"
	KeyAsyncWrite            = "asyncWrite"
	KeyConnectionsPerHost    = "connectionsPerHost"
	KeyMaxPendingConnection  = "maxPendingPerConnection"
)

// CassandraWriterJob Cassandra写入作业，复制Java实现行为
type CassandraWriterJob struct {
	config config.Configuration
	factory *factory.DataXFactory
}

func NewCassandraWriterJob() *CassandraWriterJob {
	return &CassandraWriterJob{
		factory: factory.GetGlobalFactory(),
	}
}

func (job *CassandraWriterJob) Init(config config.Configuration) error {
	job.config = config
	return nil
}

func (job *CassandraWriterJob) Prepare() error {
	return nil
}

func (job *CassandraWriterJob) Post() error {
	return nil
}

func (job *CassandraWriterJob) Destroy() error {
	return nil
}

// Split 任务分割，与Java版本匹配，简单复制配置
func (job *CassandraWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	var splitResultConfigs []config.Configuration
	for i := 0; i < mandatoryNumber; i++ {
		splitResultConfigs = append(splitResultConfigs, job.config.Clone())
	}
	return splitResultConfigs, nil
}

// CassandraWriterTask Cassandra写入任务，复制Java实现
type CassandraWriterTask struct {
	config           config.Configuration
	session          *gocql.Session
	cluster          *gocql.ClusterConfig
	preparedStmt     *gocql.Query
	columnNumber     int
	columnTypes      []gocql.TypeInfo
	columnMeta       []string
	writeTimeCol     int
	asyncWrite       bool
	batchSize        int
	batchStmts       []*gocql.Query
	factory          *factory.DataXFactory
}

func NewCassandraWriterTask() *CassandraWriterTask {
	return &CassandraWriterTask{
		factory:      factory.GetGlobalFactory(),
		writeTimeCol: -1,
	}
}

func (task *CassandraWriterTask) Init(config config.Configuration) error {
	task.config = config

	// 获取连接参数，与Java版本完全匹配
	username := config.GetString("parameter." + KeyUsername)
	password := config.GetString("parameter." + KeyPassword)
	hosts := config.GetString("parameter." + KeyHost)
	port := config.GetIntWithDefault("parameter."+KeyPort, DefaultPort)
	useSSL := config.GetBool("parameter." + KeyUseSSL)
	keyspace := config.GetString("parameter." + KeyKeyspace)
	table := config.GetString("parameter." + KeyTable)
	task.batchSize = config.GetIntWithDefault("parameter."+KeyBatchSize, DefaultBatchSize)
	task.columnMeta = config.GetStringList("parameter." + KeyColumn)
	task.columnNumber = len(task.columnMeta)
	task.asyncWrite = config.GetBool("parameter." + KeyAsyncWrite)

	connectionsPerHost := config.GetIntWithDefault("parameter."+KeyConnectionsPerHost, DefaultConnectionsPerHost)
	_ = config.GetIntWithDefault("parameter."+KeyMaxPendingConnection, DefaultMaxPendingPerConn) // 暂时不使用

	if hosts == "" {
		return fmt.Errorf("参数'%s'为必填项", KeyHost)
	}
	if keyspace == "" {
		return fmt.Errorf("参数'%s'为必填项", KeyKeyspace)
	}
	if table == "" {
		return fmt.Errorf("参数'%s'为必填项", KeyTable)
	}

	// 创建集群配置，与Java版本匹配
	task.cluster = gocql.NewCluster(strings.Split(hosts, ",")...)
	task.cluster.Port = port
	task.cluster.Keyspace = keyspace
	task.cluster.ConnectTimeout = DefaultConnectTimeout
	task.cluster.Timeout = DefaultSocketTimeout

	// 配置连接池，与Java版本匹配
	task.cluster.NumConns = connectionsPerHost

	// 配置认证
	if username != "" {
		task.cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	// 配置SSL
	if useSSL {
		task.cluster.SslOpts = &gocql.SslOptions{
			Config: &tls.Config{InsecureSkipVerify: true},
		}
	}

	// 创建会话
	var err error
	task.session, err = task.cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("创建Cassandra会话失败: %v", err)
	}

	// 获取表元数据并构建INSERT语句，与Java版本匹配
	err = task.prepareInsertStatement(keyspace, table)
	if err != nil {
		return err
	}

	// 初始化批处理相关结构
	if task.batchSize > 1 {
		task.batchStmts = make([]*gocql.Query, 0, task.batchSize)
	}

	return nil
}

// prepareInsertStatement 准备INSERT语句，复制Java版本逻辑
func (task *CassandraWriterTask) prepareInsertStatement(keyspace, table string) error {
	// 获取表的列信息
	columnTypeMap := make(map[string]gocql.TypeInfo)
	iter := task.session.Query(`
		SELECT column_name, type
		FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ?
	`, keyspace, table).Iter()

	var columnName, columnType string
	for iter.Scan(&columnName, &columnType) {
		// 简化的类型映射，实际应该解析完整的类型信息
		columnTypeMap[columnName] = parseColumnType(columnType)
	}
	iter.Close()

	// 构建INSERT语句，与Java版本匹配
	var insertColumns []string
	var placeholders []string
	task.columnTypes = make([]gocql.TypeInfo, 0, len(task.columnMeta))

	for i, columnName := range task.columnMeta {
		// 检查是否是writetime列，与Java版本匹配
		if strings.ToLower(columnName) == WritetimeColumn {
			if task.writeTimeCol != -1 {
				return fmt.Errorf("列配置信息有错误. 只能有一个时间戳列(writetime())")
			}
			task.writeTimeCol = i
			continue
		}

		colType, exists := columnTypeMap[columnName]
		if !exists {
			return fmt.Errorf("列配置信息有错误. 表中未找到列名 '%s'", columnName)
		}

		insertColumns = append(insertColumns, columnName)
		placeholders = append(placeholders, "?")
		task.columnTypes = append(task.columnTypes, colType)
	}

	// 构建INSERT语句
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(insertColumns, ","),
		strings.Join(placeholders, ","))

	// 添加timestamp支持，与Java版本匹配
	if task.writeTimeCol != -1 {
		insertSQL += " USING TIMESTAMP ?"
	}

	// 设置一致性级别
	consistencyLevel := task.config.GetString("parameter." + KeyConsistencyLevel)
	if consistencyLevel == "" {
		consistencyLevel = DefaultConsistency
	}
	consistency := parseConsistency(consistencyLevel)

	// 准备语句
	task.preparedStmt = task.session.Query(insertSQL).Consistency(consistency)

	logger.App().Info("Cassandra INSERT语句已准备", zap.String("sql", insertSQL))

	return nil
}

func (task *CassandraWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			return fmt.Errorf("从Reader获取数据失败: %v", err)
		}
		if record == nil {
			break
		}

		// 验证列数，与Java版本匹配
		if record.GetColumnNumber() != task.columnNumber {
			return fmt.Errorf("列配置信息有错误. 因为您配置的任务中，源头读取字段数:%d 与 目的表要写入的字段数:%d 不相等. 请检查您的配置并作出修改",
				record.GetColumnNumber(), task.columnNumber)
		}

		// 准备绑定值
		values, err := task.prepareValues(record)
		if err != nil {
			return fmt.Errorf("准备写入值失败: %v", err)
		}

		// 执行写入
		if task.batchSize <= 1 {
			// 单条写入
			err = task.preparedStmt.Bind(values...).Exec()
			if err != nil {
				return fmt.Errorf("写入数据失败: %v", err)
			}
		} else {
			// 批量写入
			task.batchStmts = append(task.batchStmts, task.preparedStmt.Bind(values...))

			if len(task.batchStmts) >= task.batchSize {
				err = task.executeBatch()
				if err != nil {
					return err
				}
			}
		}
	}

	// 处理剩余的批量语句
	if len(task.batchStmts) > 0 {
		err := task.executeBatch()
		if err != nil {
			return err
		}
	}

	return nil
}

// prepareValues 准备绑定值，复制Java版本的类型转换逻辑
func (task *CassandraWriterTask) prepareValues(record element.Record) ([]interface{}, error) {
	var values []interface{}

	for i := 0; i < task.columnNumber; i++ {
		// 跳过writetime列，与Java版本匹配
		if task.writeTimeCol != -1 && i == task.writeTimeCol {
			continue
		}

		column := record.GetColumn(i)
		pos := i
		if task.writeTimeCol != -1 && pos > task.writeTimeCol {
			pos = i - 1
		}

		value, err := task.convertColumnValue(column, task.columnTypes[pos])
		if err != nil {
			return nil, fmt.Errorf("转换列值失败: %v", err)
		}
		values = append(values, value)
	}

	// 添加timestamp值，与Java版本匹配
	if task.writeTimeCol != -1 {
		timestampColumn := record.GetColumn(task.writeTimeCol)
		if timestampColumn.GetRawData() != nil {
			longVal, _ := timestampColumn.GetAsLong()
			values = append(values, longVal)
		} else {
			values = append(values, time.Now().UnixNano()/1000) // 微秒时间戳
		}
	}

	return values, nil
}

// convertColumnValue 转换列值，复制Java版本的转换逻辑
func (task *CassandraWriterTask) convertColumnValue(column element.Column, columnType gocql.TypeInfo) (interface{}, error) {
	if column.GetRawData() == nil {
		return nil, nil
	}

	switch columnType.Type() {
	case gocql.TypeAscii, gocql.TypeText, gocql.TypeVarchar:
		return column.GetAsString(), nil

	case gocql.TypeBlob:
		bytes, _ := column.GetAsBytes()
		return bytes, nil

	case gocql.TypeBoolean:
		bool, _ := column.GetAsBool()
		return bool, nil

	case gocql.TypeTinyInt:
		longVal, _ := column.GetAsLong()
		return int8(longVal), nil

	case gocql.TypeSmallInt:
		longVal, _ := column.GetAsLong()
		return int16(longVal), nil

	case gocql.TypeInt:
		longVal, _ := column.GetAsLong()
		return int32(longVal), nil

	case gocql.TypeBigInt:
		longVal, _ := column.GetAsLong()
		return longVal, nil

	case gocql.TypeVarint:
		longVal, _ := column.GetAsLong()
		return big.NewInt(longVal), nil

	case gocql.TypeFloat:
		doubleVal, _ := column.GetAsDouble()
		return float32(doubleVal), nil

	case gocql.TypeDouble:
		doubleVal, _ := column.GetAsDouble()
		return doubleVal, nil

	case gocql.TypeDecimal:
		doubleVal, _ := column.GetAsDouble()
		return big.NewFloat(doubleVal), nil

	case gocql.TypeDate:
		date, _ := column.GetAsDate()
		return time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC), nil

	case gocql.TypeTime:
		longVal, _ := column.GetAsLong()
		return longVal, nil

	case gocql.TypeTimestamp:
		date, _ := column.GetAsDate()
		return date, nil

	case gocql.TypeUUID, gocql.TypeTimeUUID:
		uuidStr := column.GetAsString()
		uuid, err := gocql.ParseUUID(uuidStr)
		if err != nil {
			return nil, fmt.Errorf("解析UUID失败: %v", err)
		}
		return uuid, nil

	case gocql.TypeInet:
		ipStr := column.GetAsString()
		ip := net.ParseIP(ipStr)
		if ip == nil {
			return nil, fmt.Errorf("解析IP地址失败: %s", ipStr)
		}
		return ip, nil

	case gocql.TypeDuration:
		// Duration类型处理
		return column.GetAsString(), nil

	case gocql.TypeList:
		// 解析JSON字符串为列表
		var list []interface{}
		err := json.Unmarshal([]byte(column.GetAsString()), &list)
		if err != nil {
			return nil, fmt.Errorf("解析List类型失败: %v", err)
		}
		return list, nil

	case gocql.TypeMap:
		// 解析JSON字符串为Map
		var m map[string]interface{}
		err := json.Unmarshal([]byte(column.GetAsString()), &m)
		if err != nil {
			return nil, fmt.Errorf("解析Map类型失败: %v", err)
		}
		return m, nil

	case gocql.TypeSet:
		// 解析JSON字符串为Set（Go中用slice表示）
		var set []interface{}
		err := json.Unmarshal([]byte(column.GetAsString()), &set)
		if err != nil {
			return nil, fmt.Errorf("解析Set类型失败: %v", err)
		}
		return set, nil

	case gocql.TypeTuple, gocql.TypeUDT:
		// 复杂类型处理，暂时返回字符串
		return column.GetAsString(), nil

	default:
		return column.GetAsString(), nil
	}
}

// executeBatch 执行批量写入，与Java版本匹配
func (task *CassandraWriterTask) executeBatch() error {
	if len(task.batchStmts) == 0 {
		return nil
	}

	if task.asyncWrite {
		// 异步写入模式
		for _, stmt := range task.batchStmts {
			err := stmt.Exec()
			if err != nil {
				logger.App().Error("异步写入失败", zap.Error(err))
				return err
			}
		}
	} else {
		// 批量写入模式
		batch := task.session.NewBatch(gocql.UnloggedBatch)
		for _, stmt := range task.batchStmts {
			batch.Query(stmt.Statement(), stmt.Values()...)
		}

		err := task.session.ExecuteBatch(batch)
		if err != nil {
			logger.App().Error("batch写入失败，尝试逐条写入", zap.Error(err))
			// 批量失败时逐条重试，与Java版本匹配
			for _, stmt := range task.batchStmts {
				err = stmt.Exec()
				if err != nil {
					return fmt.Errorf("逐条写入失败: %v", err)
				}
			}
		}
	}

	// 清空批处理缓存
	task.batchStmts = task.batchStmts[:0]
	return nil
}

func (task *CassandraWriterTask) Prepare() error {
	return nil
}

func (task *CassandraWriterTask) Post() error {
	return nil
}

func (task *CassandraWriterTask) Destroy() error {
	if task.session != nil {
		task.session.Close()
	}
	return nil
}

// parseConsistency 解析一致性级别字符串，与reader保持一致
func parseConsistency(level string) gocql.Consistency {
	switch strings.ToUpper(level) {
	case "ANY":
		return gocql.Any
	case "ONE":
		return gocql.One
	case "TWO":
		return gocql.Two
	case "THREE":
		return gocql.Three
	case "QUORUM":
		return gocql.Quorum
	case "ALL":
		return gocql.All
	case "LOCAL_QUORUM":
		return gocql.LocalQuorum
	case "EACH_QUORUM":
		return gocql.EachQuorum
	case "LOCAL_ONE":
		return gocql.LocalOne
	default:
		return gocql.LocalQuorum
	}
}

// simpleTypeInfo 简化的类型信息实现
type simpleTypeInfo struct {
	dataType gocql.Type
}

func (t *simpleTypeInfo) Type() gocql.Type {
	return t.dataType
}

func (t *simpleTypeInfo) Version() byte {
	return 0
}

func (t *simpleTypeInfo) Custom() string {
	return ""
}

func (t *simpleTypeInfo) New() interface{} {
	return nil
}

func (t *simpleTypeInfo) NewWithError() (interface{}, error) {
	return nil, nil
}

// parseColumnType 简化的列类型解析
func parseColumnType(typeStr string) gocql.TypeInfo {
	// 这里是简化实现，实际应该解析完整的CQL类型信息
	switch strings.ToLower(typeStr) {
	case "text", "varchar", "ascii":
		return &simpleTypeInfo{dataType: gocql.TypeText}
	case "int":
		return &simpleTypeInfo{dataType: gocql.TypeInt}
	case "bigint":
		return &simpleTypeInfo{dataType: gocql.TypeBigInt}
	case "boolean":
		return &simpleTypeInfo{dataType: gocql.TypeBoolean}
	case "timestamp":
		return &simpleTypeInfo{dataType: gocql.TypeTimestamp}
	case "uuid":
		return &simpleTypeInfo{dataType: gocql.TypeUUID}
	case "double":
		return &simpleTypeInfo{dataType: gocql.TypeDouble}
	case "float":
		return &simpleTypeInfo{dataType: gocql.TypeFloat}
	case "blob":
		return &simpleTypeInfo{dataType: gocql.TypeBlob}
	default:
		return &simpleTypeInfo{dataType: gocql.TypeText}
	}
}