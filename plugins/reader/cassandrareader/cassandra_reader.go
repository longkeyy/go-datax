package cassandrareader

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"strconv"
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
	DefaultPort           = 9042
	DefaultConsistency    = "LOCAL_QUORUM"
	DefaultConnectTimeout = 5 * time.Second
	DefaultSocketTimeout  = 10 * time.Second
)

// Configuration keys matching Java implementation exactly
const (
	KeyUsername        = "username"
	KeyPassword        = "password"
	KeyHost            = "host"
	KeyPort            = "port"
	KeyUseSSL          = "useSSL"
	KeyKeyspace        = "keyspace"
	KeyTable           = "table"
	KeyColumn          = "column"
	KeyWhere           = "where"
	KeyAllowFiltering  = "allowFiltering"
	KeyConsistencyLevel = "consistancyLevel"
	KeyMinToken        = "minToken"
	KeyMaxToken        = "maxToken"
)

// CassandraReaderJob Cassandra读取作业，复制Java实现行为
type CassandraReaderJob struct {
	config  config.Configuration
	cluster *gocql.ClusterConfig
	factory *factory.DataXFactory
}

func NewCassandraReaderJob() *CassandraReaderJob {
	return &CassandraReaderJob{
		factory: factory.GetGlobalFactory(),
	}
}

func (job *CassandraReaderJob) Init(config config.Configuration) error {
	job.config = config

	// 获取连接参数，与Java版本完全匹配
	username := config.GetString("parameter." + KeyUsername)
	password := config.GetString("parameter." + KeyPassword)
	hosts := config.GetString("parameter." + KeyHost)
	port := config.GetIntWithDefault("parameter."+KeyPort, DefaultPort)
	useSSL := config.GetBool("parameter." + KeyUseSSL)

	if hosts == "" {
		return fmt.Errorf("参数'%s'为必填项", KeyHost)
	}

	// 创建集群配置，与Java实现匹配
	job.cluster = gocql.NewCluster(strings.Split(hosts, ",")...)
	job.cluster.Port = port
	job.cluster.ConnectTimeout = DefaultConnectTimeout
	job.cluster.Timeout = DefaultSocketTimeout

	// 配置认证，与Java版本行为一致
	if username != "" {
		job.cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	// 配置SSL，与Java版本匹配
	if useSSL {
		job.cluster.SslOpts = &gocql.SslOptions{
			Config: &tls.Config{InsecureSkipVerify: true},
		}
	}

	// 验证连接和配置
	return job.checkConfig()
}

func (job *CassandraReaderJob) checkConfig() error {
	// 验证必填参数，与Java版本完全匹配
	keyspace := job.config.GetString("parameter." + KeyKeyspace)
	if keyspace == "" {
		return fmt.Errorf("参数'%s'为必填项", KeyKeyspace)
	}

	table := job.config.GetString("parameter." + KeyTable)
	if table == "" {
		return fmt.Errorf("参数'%s'为必填项", KeyTable)
	}

	columns := job.config.GetStringList("parameter." + KeyColumn)
	if len(columns) == 0 {
		return fmt.Errorf("参数'%s'为必填项", KeyColumn)
	}

	// 验证列名不为空，与Java版本匹配
	for _, col := range columns {
		if col == "" {
			return fmt.Errorf("配置信息有错误.列信息中需要包含'name'字段")
		}
	}

	// 连接验证keyspace和table是否存在，与Java版本行为一致
	session, err := job.cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("连接Cassandra失败: %v", err)
	}
	defer session.Close()

	// 验证keyspace存在
	var count int
	err = session.Query("SELECT COUNT(*) FROM system_schema.keyspaces WHERE keyspace_name = ?", keyspace).Scan(&count)
	if err != nil || count == 0 {
		return fmt.Errorf("配置信息有错误.keyspace'%s'不存在", keyspace)
	}

	// 验证table存在
	err = session.Query("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?", keyspace, table).Scan(&count)
	if err != nil || count == 0 {
		return fmt.Errorf("配置信息有错误.表'%s'不存在", table)
	}

	return nil
}

func (job *CassandraReaderJob) Post() error {
	return nil
}

func (job *CassandraReaderJob) Destroy() error {
	return nil
}

// Split 任务分割，复制Java版本的分割逻辑
func (job *CassandraReaderJob) Split(adviceNumber int) ([]config.Configuration, error) {
	var splittedConfigs []config.Configuration

	if adviceNumber <= 1 {
		splittedConfigs = append(splittedConfigs, job.config)
		return splittedConfigs, nil
	}

	// 检查是否存在自定义token查询，与Java版本逻辑一致
	where := job.config.GetString("parameter." + KeyWhere)
	if where != "" && strings.Contains(strings.ToLower(where), "token(") {
		splittedConfigs = append(splittedConfigs, job.config)
		return splittedConfigs, nil
	}

	// 获取分区器类型并进行token分割，与Java版本完全匹配
	session, err := job.cluster.CreateSession()
	if err != nil {
		splittedConfigs = append(splittedConfigs, job.config)
		return splittedConfigs, nil
	}
	defer session.Close()

	var partitioner string
	err = session.Query("SELECT partitioner FROM system.local").Scan(&partitioner)
	if err != nil {
		splittedConfigs = append(splittedConfigs, job.config)
		return splittedConfigs, nil
	}

	logger.App().Info("Cassandra分区器类型", zap.String("partitioner", partitioner))

	// 根据分区器类型进行分割，与Java版本逻辑匹配
	if strings.HasSuffix(partitioner, "RandomPartitioner") {
		return job.splitRandomPartitioner(adviceNumber)
	} else if strings.HasSuffix(partitioner, "Murmur3Partitioner") {
		return job.splitMurmur3Partitioner(adviceNumber)
	}

	// 不支持的分区器，不分割
	splittedConfigs = append(splittedConfigs, job.config)
	return splittedConfigs, nil
}

// splitRandomPartitioner 复制Java版本的RandomPartitioner分割逻辑
func (job *CassandraReaderJob) splitRandomPartitioner(adviceNumber int) ([]config.Configuration, error) {
	var splittedConfigs []config.Configuration

	// Java版本的token范围: -1 到 2^127
	minToken := big.NewInt(-1)
	maxToken := new(big.Int).Exp(big.NewInt(2), big.NewInt(127), nil)

	step := new(big.Int).Sub(maxToken, minToken)
	step = step.Div(step, big.NewInt(int64(adviceNumber)))

	for i := 0; i < adviceNumber; i++ {
		left := new(big.Int).Add(minToken, new(big.Int).Mul(step, big.NewInt(int64(i))))
		right := new(big.Int).Add(minToken, new(big.Int).Mul(step, big.NewInt(int64(i+1))))

		if i == adviceNumber-1 {
			right = maxToken
		}

		taskConfig := job.config.Clone()
		taskConfig.Set("parameter."+KeyMinToken, left.String())
		taskConfig.Set("parameter."+KeyMaxToken, right.String())
		splittedConfigs = append(splittedConfigs, taskConfig)
	}

	return splittedConfigs, nil
}

// splitMurmur3Partitioner 复制Java版本的Murmur3Partitioner分割逻辑
func (job *CassandraReaderJob) splitMurmur3Partitioner(adviceNumber int) ([]config.Configuration, error) {
	var splittedConfigs []config.Configuration

	// Java版本的token范围: Long.MIN_VALUE 到 Long.MAX_VALUE
	minToken := int64(-9223372036854775808) // Long.MIN_VALUE
	maxToken := int64(9223372036854775807)  // Long.MAX_VALUE

	step := (maxToken - minToken) / int64(adviceNumber)

	for i := 0; i < adviceNumber; i++ {
		left := minToken + step*int64(i)
		right := minToken + step*int64(i+1)

		if i == adviceNumber-1 {
			right = maxToken
		}

		taskConfig := job.config.Clone()
		taskConfig.Set("parameter."+KeyMinToken, strconv.FormatInt(left, 10))
		taskConfig.Set("parameter."+KeyMaxToken, strconv.FormatInt(right, 10))
		splittedConfigs = append(splittedConfigs, taskConfig)
	}

	return splittedConfigs, nil
}

// CassandraReaderTask Cassandra读取任务，复制Java实现
type CassandraReaderTask struct {
	config           config.Configuration
	session          *gocql.Session
	cluster          *gocql.ClusterConfig
	queryString      string
	consistency      gocql.Consistency
	columnNumber     int
	columnMeta       []string
	factory          *factory.DataXFactory
}

func NewCassandraReaderTask() *CassandraReaderTask {
	return &CassandraReaderTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *CassandraReaderTask) Init(config config.Configuration) error {
	task.config = config

	// 获取连接参数，与Java版本完全匹配
	username := config.GetString("parameter." + KeyUsername)
	password := config.GetString("parameter." + KeyPassword)
	hosts := config.GetString("parameter." + KeyHost)
	port := config.GetIntWithDefault("parameter."+KeyPort, DefaultPort)
	useSSL := config.GetBool("parameter." + KeyUseSSL)
	keyspace := config.GetString("parameter." + KeyKeyspace)
	task.columnMeta = config.GetStringList("parameter." + KeyColumn)
	task.columnNumber = len(task.columnMeta)

	// 创建集群配置
	task.cluster = gocql.NewCluster(strings.Split(hosts, ",")...)
	task.cluster.Port = port
	task.cluster.Keyspace = keyspace
	task.cluster.ConnectTimeout = DefaultConnectTimeout
	task.cluster.Timeout = DefaultSocketTimeout

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

	// 设置一致性级别，与Java版本匹配
	consistencyLevel := config.GetString("parameter." + KeyConsistencyLevel)
	if consistencyLevel == "" {
		consistencyLevel = DefaultConsistency
	}
	task.consistency = parseConsistency(consistencyLevel)

	// 构建查询语句，与Java版本完全匹配
	task.queryString = task.getQueryString()
	logger.App().Info("Cassandra查询语句", zap.String("query", task.queryString))

	return nil
}

// getQueryString 构建查询语句，复制Java版本逻辑
func (task *CassandraReaderTask) getQueryString() string {
	table := task.config.GetString("parameter." + KeyTable)

	// 构建列名
	columns := strings.Join(task.columnMeta, ",")

	// 构建WHERE条件
	var whereClauses []string

	// 添加用户自定义WHERE条件
	where := task.config.GetString("parameter." + KeyWhere)
	if where != "" {
		whereClauses = append(whereClauses, where)
	}

	// 添加token范围条件，与Java版本逻辑匹配
	minToken := task.config.GetString("parameter." + KeyMinToken)
	maxToken := task.config.GetString("parameter." + KeyMaxToken)

	if minToken != "" || maxToken != "" {
		logger.App().Info("Token范围", zap.String("minToken", minToken), zap.String("maxToken", maxToken))

		// 获取分区键，与Java版本匹配
		partitionKeys := task.getPartitionKeys()
		if len(partitionKeys) > 0 {
			pkString := strings.Join(partitionKeys, ",")

			if minToken != "" {
				whereClauses = append(whereClauses, fmt.Sprintf("token(%s) > %s", pkString, minToken))
			}
			if maxToken != "" {
				whereClauses = append(whereClauses, fmt.Sprintf("token(%s) <= %s", pkString, maxToken))
			}
		}
	}

	// 构建完整查询
	query := fmt.Sprintf("SELECT %s FROM %s", columns, table)

	if len(whereClauses) > 0 {
		query += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	// 添加ALLOW FILTERING，与Java版本匹配
	allowFiltering := task.config.GetBool("parameter." + KeyAllowFiltering)
	if allowFiltering {
		query += " ALLOW FILTERING"
	}

	return query + ";"
}

// getPartitionKeys 获取分区键，用于token范围查询
func (task *CassandraReaderTask) getPartitionKeys() []string {
	keyspace := task.config.GetString("parameter." + KeyKeyspace)
	table := task.config.GetString("parameter." + KeyTable)

	// 查询分区键信息
	var partitionKeys []string
	iter := task.session.Query(`
		SELECT column_name
		FROM system_schema.columns
		WHERE keyspace_name = ? AND table_name = ? AND kind = 'partition_key'
		ORDER BY position
	`, keyspace, table).Iter()

	var columnName string
	for iter.Scan(&columnName) {
		partitionKeys = append(partitionKeys, columnName)
	}
	iter.Close()

	return partitionKeys
}

func (task *CassandraReaderTask) StartRead(recordSender plugin.RecordSender) error {
	// 执行查询，与Java版本匹配
	query := task.session.Query(task.queryString).Consistency(task.consistency)
	iter := query.Iter()

	// 获取列信息
	columns := iter.Columns()

	// 读取数据行
	for {
		values := make([]interface{}, len(columns))
		if !iter.Scan(values...) {
			break
		}

		// 构建Record，与Java版本行为匹配
		record := task.buildRecord(values, columns, recordSender)
		if record != nil {
			err := recordSender.SendRecord(record)
			if err != nil {
				return fmt.Errorf("发送数据失败: %v", err)
			}
		}
	}

	if err := iter.Close(); err != nil {
		return fmt.Errorf("查询执行失败: %v", err)
	}

	return nil
}

// buildRecord 构建Record，复制Java版本的数据类型转换逻辑
func (task *CassandraReaderTask) buildRecord(values []interface{}, columns []gocql.ColumnInfo, recordSender plugin.RecordSender) element.Record {
	record := element.NewRecord()

	for i := 0; i < task.columnNumber && i < len(values); i++ {
		value := values[i]
		columnInfo := columns[i]

		if value == nil {
			record.AddColumn(element.NewStringColumn(""))
			continue
		}

		// 根据数据类型转换，与Java版本匹配
		switch columnInfo.TypeInfo.Type() {
		case gocql.TypeAscii, gocql.TypeText, gocql.TypeVarchar:
			record.AddColumn(element.NewStringColumn(value.(string)))

		case gocql.TypeBlob:
			if bytes, ok := value.([]byte); ok {
				record.AddColumn(element.NewBytesColumn(bytes))
			} else {
				record.AddColumn(element.NewStringColumn(""))
			}

		case gocql.TypeBoolean:
			record.AddColumn(element.NewBoolColumn(value.(bool)))

		case gocql.TypeTinyInt:
			record.AddColumn(element.NewLongColumn(int64(value.(int8))))

		case gocql.TypeSmallInt:
			record.AddColumn(element.NewLongColumn(int64(value.(int16))))

		case gocql.TypeInt:
			record.AddColumn(element.NewLongColumn(int64(value.(int32))))

		case gocql.TypeBigInt, gocql.TypeCounter:
			record.AddColumn(element.NewLongColumn(value.(int64)))

		case gocql.TypeVarint:
			if bigInt, ok := value.(*big.Int); ok {
				record.AddColumn(element.NewLongColumn(bigInt.Int64()))
			} else {
				record.AddColumn(element.NewStringColumn(""))
			}

		case gocql.TypeFloat:
			record.AddColumn(element.NewDoubleColumn(float64(value.(float32))))

		case gocql.TypeDouble:
			record.AddColumn(element.NewDoubleColumn(value.(float64)))

		case gocql.TypeDecimal:
			// Cassandra decimal类型转换
			record.AddColumn(element.NewDoubleColumn(value.(float64)))

		case gocql.TypeDate:
			// Cassandra LocalDate转换为毫秒时间戳
			if t, ok := value.(time.Time); ok {
				record.AddColumn(element.NewDateColumn(t))
			} else {
				record.AddColumn(element.NewStringColumn(""))
			}

		case gocql.TypeTime:
			record.AddColumn(element.NewLongColumn(value.(int64)))

		case gocql.TypeTimestamp:
			if t, ok := value.(time.Time); ok {
				record.AddColumn(element.NewDateColumn(t))
			} else {
				record.AddColumn(element.NewStringColumn(""))
			}

		case gocql.TypeUUID, gocql.TypeTimeUUID:
			if uuid, ok := value.(gocql.UUID); ok {
				record.AddColumn(element.NewStringColumn(uuid.String()))
			} else {
				record.AddColumn(element.NewStringColumn(""))
			}

		case gocql.TypeInet:
			if ip, ok := value.(net.IP); ok {
				record.AddColumn(element.NewStringColumn(ip.String()))
			} else {
				record.AddColumn(element.NewStringColumn(""))
			}

		case gocql.TypeDuration:
			record.AddColumn(element.NewStringColumn(fmt.Sprintf("%v", value)))

		case gocql.TypeList, gocql.TypeMap, gocql.TypeSet, gocql.TypeTuple, gocql.TypeUDT:
			// 复杂类型转换为JSON字符串，与Java版本匹配
			jsonStr := task.toJSONString(value, columnInfo.TypeInfo)
			record.AddColumn(element.NewStringColumn(jsonStr))

		default:
			// 未支持的类型，转换为字符串
			record.AddColumn(element.NewStringColumn(fmt.Sprintf("%v", value)))
		}
	}

	return record
}

// toJSONString 将复杂类型转换为JSON字符串，复制Java版本逻辑
func (task *CassandraReaderTask) toJSONString(value interface{}, typeInfo gocql.TypeInfo) string {
	if value == nil {
		return "null"
	}

	switch typeInfo.Type() {
	case gocql.TypeList:
		if list, ok := value.([]interface{}); ok {
			jsonBytes, _ := json.Marshal(list)
			return string(jsonBytes)
		}

	case gocql.TypeMap:
		if m, ok := value.(map[interface{}]interface{}); ok {
			// 转换map的key为字符串类型
			stringMap := make(map[string]interface{})
			for k, v := range m {
				keyStr := fmt.Sprintf("%v", k)
				stringMap[keyStr] = v
			}
			jsonBytes, _ := json.Marshal(stringMap)
			return string(jsonBytes)
		}

	case gocql.TypeSet:
		if set, ok := value.([]interface{}); ok {
			jsonBytes, _ := json.Marshal(set)
			return string(jsonBytes)
		}

	case gocql.TypeBlob:
		if bytes, ok := value.([]byte); ok {
			return base64.StdEncoding.EncodeToString(bytes)
		}
	}

	// 其他类型直接转换为JSON
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(jsonBytes)
}

func (task *CassandraReaderTask) Post() error {
	return nil
}

func (task *CassandraReaderTask) Destroy() error {
	if task.session != nil {
		task.session.Close()
	}
	return nil
}

// parseConsistency 解析一致性级别字符串，与Java版本匹配
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
		return gocql.LocalQuorum // 默认值与Java版本匹配
	}
}