package elasticsearchwriter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/factory"
	"github.com/longkeyy/go-datax/common/logger"
	"github.com/longkeyy/go-datax/common/plugin"
	coreplugin "github.com/longkeyy/go-datax/core/registry"
	"go.uber.org/zap"
)

const (
	DefaultBatchSize     = 1024
	DefaultTimeout       = 600
	DefaultTrySize       = 30
	DefaultTryInterval   = 60000
	DefaultSplitter      = "-,-"
	DefaultActionType    = "index"
	DefaultESVersion     = 7
)

// ActionType 操作类型枚举
type ActionType string

const (
	ActionTypeIndex  ActionType = "index"
	ActionTypeCreate ActionType = "create"
	ActionTypeDelete ActionType = "delete"
	ActionTypeUpdate ActionType = "update"
)

// ESFieldType ElasticSearch字段类型
type ESFieldType string

const (
	FieldTypeID          ESFieldType = "id"
	FieldTypeVersion     ESFieldType = "version"
	FieldTypeRouting     ESFieldType = "routing"
	FieldTypeParent      ESFieldType = "parent"
	FieldTypeString      ESFieldType = "string"
	FieldTypeKeyword     ESFieldType = "keyword"
	FieldTypeText        ESFieldType = "text"
	FieldTypeDate        ESFieldType = "date"
	FieldTypeLong        ESFieldType = "long"
	FieldTypeInteger     ESFieldType = "integer"
	FieldTypeShort       ESFieldType = "short"
	FieldTypeByte        ESFieldType = "byte"
	FieldTypeDouble      ESFieldType = "double"
	FieldTypeFloat       ESFieldType = "float"
	FieldTypeBoolean     ESFieldType = "boolean"
	FieldTypeBinary      ESFieldType = "binary"
	FieldTypeObject      ESFieldType = "object"
	FieldTypeNested      ESFieldType = "nested"
	FieldTypeGeoPoint    ESFieldType = "geo_point"
	FieldTypeGeoShape    ESFieldType = "geo_shape"
	FieldTypeIP          ESFieldType = "ip"
	FieldTypeIPRange     ESFieldType = "ip_range"
	FieldTypeDateRange   ESFieldType = "date_range"
	FieldTypeIntegerRange ESFieldType = "integer_range"
	FieldTypeFloatRange  ESFieldType = "float_range"
	FieldTypeLongRange   ESFieldType = "long_range"
	FieldTypeDoubleRange ESFieldType = "double_range"
)

// ESColumn ElasticSearch列配置
type ESColumn struct {
	Name                         string      `json:"name"`
	Type                         string      `json:"type"`
	Array                        bool        `json:"array"`
	DstArray                     bool        `json:"dstArray"`
	JsonArray                    bool        `json:"json_array"`
	CombineFields                []string    `json:"combineFields"`
	CombineFieldsValueSeparator  string      `json:"combineFieldsValueSeparator"`
	Format                       string      `json:"format"`
	Timezone                     string      `json:"timezone"`
	Origin                       bool        `json:"origin"`
}

// PrimaryKeyInfo 主键信息配置
type PrimaryKeyInfo struct {
	Column          []string `json:"column"`
	FieldDelimiter  string   `json:"fieldDelimiter"`
}

// PartitionColumn 分区列配置
type PartitionColumn struct {
	Name string `json:"name"`
}

// ElasticSearchWriterJob ElasticSearch写入作业
type ElasticSearchWriterJob struct {
	config            config.Configuration
	endpoint          string
	username          string
	password          string
	index             string
	indexType         string
	truncate          bool
	discovery         bool
	compression       bool
	multiThread       bool
	timeout           int
	dynamic           bool
	dstDynamic        string
	settings          map[string]interface{}
	columns           []ESColumn
	actionType        ActionType
	alias             string
	needCleanAlias    bool
	esVersion         int
	batchSize         int
	trySize           int
	tryInterval       int64
	enableWriteNull   bool
	ignoreWriteError  bool
	ignoreParseError  bool
	deleteBy          []map[string]interface{}
	urlParams         map[string]interface{}
	fieldDelimiter    string
	primaryKeyInfo    *PrimaryKeyInfo
	esPartitionColumn []PartitionColumn
	splitter          string
	factory           *factory.DataXFactory
	client            *elasticsearch.Client
}

func NewElasticSearchWriterJob() *ElasticSearchWriterJob {
	return &ElasticSearchWriterJob{
		factory: factory.GetGlobalFactory(),
	}
}

func (job *ElasticSearchWriterJob) Init(cfg config.Configuration) error {
	job.config = cfg

	// 获取必需参数
	job.endpoint = cfg.GetString("parameter.endpoint")
	if job.endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}

	job.index = cfg.GetString("parameter.index")
	if job.index == "" {
		return fmt.Errorf("index is required")
	}

	// 获取认证信息
	job.username = cfg.GetString("parameter.username")
	job.password = cfg.GetString("parameter.password")

	// 获取索引类型，7.x以后默认为_doc
	job.indexType = cfg.GetString("parameter.indexType")
	if job.indexType == "" {
		job.indexType = cfg.GetString("parameter.type")
		if job.indexType == "" {
			job.indexType = job.index
		}
	}

	// 获取操作类型
	actionTypeStr := cfg.GetString("parameter.actionType")
	if actionTypeStr == "" {
		actionTypeStr = DefaultActionType
	}
	job.actionType = ActionType(actionTypeStr)

	// 获取可选参数
	job.truncate = cfg.GetBool("parameter.truncate")
	job.discovery = cfg.GetBool("parameter.discovery")
	job.compression = cfg.GetBool("parameter.compression")
	if !cfg.IsExists("parameter.compression") {
		job.compression = true // 默认开启压缩
	}
	job.multiThread = cfg.GetBool("parameter.multiThread")
	if !cfg.IsExists("parameter.multiThread") {
		job.multiThread = true // 默认开启多线程
	}

	job.timeout = cfg.GetInt("parameter.timeout")
	if job.timeout <= 0 {
		job.timeout = DefaultTimeout
	}

	job.dynamic = cfg.GetBool("parameter.dynamic")
	job.dstDynamic = cfg.GetString("parameter.dstDynamic")

	// 获取settings配置
	job.settings = cfg.GetMap("parameter.settings")
	if job.settings == nil {
		job.settings = make(map[string]interface{})
	}

	// 获取列配置
	columnConfigs := cfg.GetListConfiguration("parameter.column")
	if len(columnConfigs) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	job.columns = make([]ESColumn, len(columnConfigs))
	for i, colCfg := range columnConfigs {
		col := ESColumn{
			Name:                        colCfg.GetString("name"),
			Type:                        colCfg.GetString("type"),
			Array:                       colCfg.GetBool("array"),
			DstArray:                    colCfg.GetBool("dstArray"),
			JsonArray:                   colCfg.GetBool("json_array"),
			CombineFields:               colCfg.GetStringList("combineFields"),
			CombineFieldsValueSeparator: colCfg.GetString("combineFieldsValueSeparator"),
			Format:                      colCfg.GetString("format"),
			Timezone:                    colCfg.GetString("timezone"),
			Origin:                      colCfg.GetBool("origin"),
		}
		if col.CombineFieldsValueSeparator == "" {
			col.CombineFieldsValueSeparator = ""
		}
		job.columns[i] = col
	}

	// 获取其他配置参数
	job.alias = cfg.GetString("parameter.alias")
	job.needCleanAlias = cfg.GetString("parameter.aliasMode") == "exclusive"
	job.esVersion = cfg.GetInt("parameter.esVersion")
	if job.esVersion <= 0 {
		job.esVersion = DefaultESVersion
	}

	job.batchSize = cfg.GetInt("parameter.batchSize")
	if job.batchSize <= 0 {
		job.batchSize = DefaultBatchSize
	}

	job.trySize = cfg.GetInt("parameter.trySize")
	if job.trySize <= 0 {
		job.trySize = DefaultTrySize
	}

	job.tryInterval = int64(cfg.GetInt("parameter.tryInterval"))
	if job.tryInterval <= 0 {
		job.tryInterval = DefaultTryInterval
	}

	job.enableWriteNull = cfg.GetBool("parameter.enableWriteNull")
	if !cfg.IsExists("parameter.enableWriteNull") {
		job.enableWriteNull = true // 默认允许写入null值
	}

	job.ignoreWriteError = cfg.GetBool("parameter.ignoreWriteError")
	job.ignoreParseError = cfg.GetBool("parameter.ignoreParseError")
	if !cfg.IsExists("parameter.ignoreParseError") {
		job.ignoreParseError = true // 默认忽略解析错误
	}

	// 获取删除条件
	deleteByStr := cfg.GetString("parameter.deleteBy")
	if deleteByStr != "" {
		if err := json.Unmarshal([]byte(deleteByStr), &job.deleteBy); err != nil {
			logger.Component().WithComponent("ElasticSearchWriter").Warn("Failed to parse deleteBy configuration",
				zap.Error(err))
		}
	}

	// 获取URL参数
	job.urlParams = cfg.GetMap("parameter.urlParams")
	if job.urlParams == nil {
		job.urlParams = make(map[string]interface{})
	}

	job.fieldDelimiter = cfg.GetString("parameter.fieldDelimiter")
	job.splitter = cfg.GetString("parameter.splitter")
	if job.splitter == "" {
		job.splitter = DefaultSplitter
	}

	// 获取主键信息
	primaryKeyInfoStr := cfg.GetString("parameter.primaryKeyInfo")
	if primaryKeyInfoStr != "" {
		if err := json.Unmarshal([]byte(primaryKeyInfoStr), &job.primaryKeyInfo); err != nil {
			logger.Component().WithComponent("ElasticSearchWriter").Warn("Failed to parse primaryKeyInfo configuration",
				zap.Error(err))
		}
	}

	// 获取分区列信息
	esPartitionColumnStr := cfg.GetString("parameter.esPartitionColumn")
	if esPartitionColumnStr != "" {
		if err := json.Unmarshal([]byte(esPartitionColumnStr), &job.esPartitionColumn); err != nil {
			logger.Component().WithComponent("ElasticSearchWriter").Warn("Failed to parse esPartitionColumn configuration",
				zap.Error(err))
		}
	}

	return nil
}

func (job *ElasticSearchWriterJob) Prepare() error {
	// 创建ElasticSearch客户端
	addresses := strings.Split(job.endpoint, ",")

	clientConfig := elasticsearch.Config{
		Addresses: addresses,
	}

	if job.username != "" && job.password != "" {
		clientConfig.Username = job.username
		clientConfig.Password = job.password
	}

	var err error
	job.client, err = elasticsearch.NewClient(clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	// 检查连接
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(job.timeout)*time.Second)
	defer cancel()

	res, err := job.client.Info(job.client.Info.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("failed to connect to elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch connection error: %s", res.String())
	}

	logger.Component().WithComponent("ElasticSearchWriter").Info("Connected to ElasticSearch successfully")

	// 检查索引是否存在
	indexExists, err := job.checkIndexExists()
	if err != nil {
		return fmt.Errorf("failed to check index existence: %w", err)
	}

	// 如果需要清理索引
	if job.truncate && indexExists {
		if err := job.deleteIndex(); err != nil {
			return fmt.Errorf("failed to delete index: %w", err)
		}
		indexExists = false
	}

	// 创建索引和映射
	if !indexExists {
		if err := job.createIndexWithMappings(); err != nil {
			return fmt.Errorf("failed to create index with mappings: %w", err)
		}
	}

	return nil
}

func (job *ElasticSearchWriterJob) checkIndexExists() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(job.timeout)*time.Second)
	defer cancel()

	req := esapi.IndicesExistsRequest{
		Index: []string{job.index},
	}

	res, err := req.Do(ctx, job.client)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return res.StatusCode == 200, nil
}

func (job *ElasticSearchWriterJob) deleteIndex() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(job.timeout)*time.Second)
	defer cancel()

	req := esapi.IndicesDeleteRequest{
		Index: []string{job.index},
	}

	res, err := req.Do(ctx, job.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to delete index: %s", res.String())
	}

	logger.Component().WithComponent("ElasticSearchWriter").Info("Index deleted successfully", zap.String("index", job.index))
	return nil
}

func (job *ElasticSearchWriterJob) createIndexWithMappings() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(job.timeout)*time.Second)
	defer cancel()

	// 生成mappings
	mappings := job.generateMappings()

	// 构建索引创建请求体
	indexBody := map[string]interface{}{
		"mappings": mappings,
	}

	if len(job.settings) > 0 {
		indexBody["settings"] = job.settings
	}

	body, err := json.Marshal(indexBody)
	if err != nil {
		return fmt.Errorf("failed to marshal index body: %w", err)
	}

	req := esapi.IndicesCreateRequest{
		Index: job.index,
		Body:  bytes.NewReader(body),
	}

	res, err := req.Do(ctx, job.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to create index: %s", res.String())
	}

	logger.Component().WithComponent("ElasticSearchWriter").Info("Index created successfully",
		zap.String("index", job.index),
		zap.String("mappings", string(body)))
	return nil
}

func (job *ElasticSearchWriterJob) generateMappings() map[string]interface{} {
	properties := make(map[string]interface{})

	for _, col := range job.columns {
		fieldType := ESFieldType(col.Type)

		// 跳过特殊字段类型（id, version, routing等）
		if fieldType == FieldTypeID || fieldType == FieldTypeVersion ||
		   fieldType == FieldTypeRouting || fieldType == FieldTypeParent {
			continue
		}

		fieldMapping := map[string]interface{}{
			"type": col.Type,
		}

		// 根据字段类型添加特定配置
		switch fieldType {
		case FieldTypeText:
			if col.Format != "" {
				fieldMapping["analyzer"] = col.Format
			}
		case FieldTypeDate:
			if col.Format != "" && col.Origin {
				fieldMapping["format"] = col.Format
			}
		case FieldTypeGeoShape:
			// GeoShape相关配置可以在这里扩展
		case FieldTypeObject, FieldTypeNested:
			if job.dstDynamic != "" {
				fieldMapping["dynamic"] = job.dstDynamic
			}
		}

		properties[col.Name] = fieldMapping
	}

	mappings := map[string]interface{}{
		"properties": properties,
	}

	if job.dstDynamic != "" {
		mappings["dynamic"] = job.dstDynamic
	}

	// ES 7.x+ 不再支持type，直接返回properties结构
	if job.esVersion >= 7 {
		return mappings
	} else {
		// ES 6.x及以下版本需要type包装
		return map[string]interface{}{
			job.indexType: mappings,
		}
	}
}

func (job *ElasticSearchWriterJob) Post() error {
	// 处理别名设置
	if job.alias != "" {
		if err := job.createAlias(); err != nil {
			return fmt.Errorf("failed to create alias: %w", err)
		}
	}
	return nil
}

func (job *ElasticSearchWriterJob) createAlias() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(job.timeout)*time.Second)
	defer cancel()

	var actions []map[string]interface{}

	// 如果需要清理别名，先移除现有别名
	if job.needCleanAlias {
		actions = append(actions, map[string]interface{}{
			"remove": map[string]interface{}{
				"index": "*",
				"alias": job.alias,
			},
		})
	}

	// 添加新别名
	actions = append(actions, map[string]interface{}{
		"add": map[string]interface{}{
			"index": job.index,
			"alias": job.alias,
		},
	})

	body := map[string]interface{}{
		"actions": actions,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal alias body: %w", err)
	}

	req := esapi.IndicesUpdateAliasesRequest{
		Body: bytes.NewReader(bodyBytes),
	}

	res, err := req.Do(ctx, job.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to create alias: %s", res.String())
	}

	logger.Component().WithComponent("ElasticSearchWriter").Info("Alias created successfully",
		zap.String("index", job.index), zap.String("alias", job.alias))
	return nil
}

func (job *ElasticSearchWriterJob) Destroy() error {
	// 关闭资源
	return nil
}

func (job *ElasticSearchWriterJob) Split(adviceNumber int) ([]config.Configuration, error) {
	configs := make([]config.Configuration, adviceNumber)
	for i := 0; i < adviceNumber; i++ {
		configs[i] = job.config.Clone()
	}
	return configs, nil
}

// ElasticSearchWriterTask ElasticSearch写入任务
type ElasticSearchWriterTask struct {
	config            config.Configuration
	endpoint          string
	username          string
	password          string
	index             string
	indexType         string
	actionType        ActionType
	batchSize         int
	trySize           int
	tryInterval       int64
	timeout           int
	enableWriteNull   bool
	ignoreWriteError  bool
	ignoreParseError  bool
	deleteBy          []map[string]interface{}
	urlParams         map[string]interface{}
	fieldDelimiter    string
	primaryKeyInfo    *PrimaryKeyInfo
	esPartitionColumn []PartitionColumn
	splitter          string
	columns           []ESColumn
	esVersion         int
	factory           *factory.DataXFactory
	client            *elasticsearch.Client
	colNameToIndexMap map[string]int
	hasID             bool
	hasPrimaryKeyInfo bool
	hasEsPartitionColumn bool
	combinedIdColumn  *ESColumn
}

func NewElasticSearchWriterTask() *ElasticSearchWriterTask {
	return &ElasticSearchWriterTask{
		factory: factory.GetGlobalFactory(),
		colNameToIndexMap: make(map[string]int),
	}
}

func (task *ElasticSearchWriterTask) Init(cfg config.Configuration) error {
	task.config = cfg

	// 复制job中的配置
	task.endpoint = cfg.GetString("parameter.endpoint")
	task.username = cfg.GetString("parameter.username")
	task.password = cfg.GetString("parameter.password")
	task.index = cfg.GetString("parameter.index")
	task.indexType = cfg.GetString("parameter.indexType")
	if task.indexType == "" {
		task.indexType = cfg.GetString("parameter.type")
		if task.indexType == "" {
			task.indexType = task.index
		}
	}

	actionTypeStr := cfg.GetString("parameter.actionType")
	if actionTypeStr == "" {
		actionTypeStr = DefaultActionType
	}
	task.actionType = ActionType(actionTypeStr)

	task.batchSize = cfg.GetInt("parameter.batchSize")
	if task.batchSize <= 0 {
		task.batchSize = DefaultBatchSize
	}

	task.trySize = cfg.GetInt("parameter.trySize")
	if task.trySize <= 0 {
		task.trySize = DefaultTrySize
	}

	task.tryInterval = int64(cfg.GetInt("parameter.tryInterval"))
	if task.tryInterval <= 0 {
		task.tryInterval = DefaultTryInterval
	}

	task.timeout = cfg.GetInt("parameter.timeout")
	if task.timeout <= 0 {
		task.timeout = DefaultTimeout
	}

	task.enableWriteNull = cfg.GetBool("parameter.enableWriteNull")
	if !cfg.IsExists("parameter.enableWriteNull") {
		task.enableWriteNull = true
	}

	task.ignoreWriteError = cfg.GetBool("parameter.ignoreWriteError")
	task.ignoreParseError = cfg.GetBool("parameter.ignoreParseError")
	if !cfg.IsExists("parameter.ignoreParseError") {
		task.ignoreParseError = true
	}

	task.esVersion = cfg.GetInt("parameter.esVersion")
	if task.esVersion <= 0 {
		task.esVersion = DefaultESVersion
	}

	// 获取列配置
	columnConfigs := cfg.GetListConfiguration("parameter.column")
	task.columns = make([]ESColumn, len(columnConfigs))
	for i, colCfg := range columnConfigs {
		col := ESColumn{
			Name:                        colCfg.GetString("name"),
			Type:                        colCfg.GetString("type"),
			Array:                       colCfg.GetBool("array"),
			DstArray:                    colCfg.GetBool("dstArray"),
			JsonArray:                   colCfg.GetBool("json_array"),
			CombineFields:               colCfg.GetStringList("combineFields"),
			CombineFieldsValueSeparator: colCfg.GetString("combineFieldsValueSeparator"),
			Format:                      colCfg.GetString("format"),
			Timezone:                    colCfg.GetString("timezone"),
			Origin:                      colCfg.GetBool("origin"),
		}
		if col.CombineFieldsValueSeparator == "" {
			col.CombineFieldsValueSeparator = ""
		}
		task.columns[i] = col

		// 检查是否有ID字段或组合ID字段
		if ESFieldType(col.Type) == FieldTypeID {
			task.hasID = true
			if len(col.CombineFields) > 0 {
				task.combinedIdColumn = &col
			}
		}
	}

	// 获取其他配置
	deleteByStr := cfg.GetString("parameter.deleteBy")
	if deleteByStr != "" {
		if err := json.Unmarshal([]byte(deleteByStr), &task.deleteBy); err != nil {
			logger.Component().WithComponent("ElasticSearchWriter").Warn("Failed to parse deleteBy configuration",
				zap.Error(err))
		}
	}

	task.urlParams = cfg.GetMap("parameter.urlParams")
	if task.urlParams == nil {
		task.urlParams = make(map[string]interface{})
	}

	task.fieldDelimiter = cfg.GetString("parameter.fieldDelimiter")
	task.splitter = cfg.GetString("parameter.splitter")
	if task.splitter == "" {
		task.splitter = DefaultSplitter
	}

	// 获取主键信息
	primaryKeyInfoStr := cfg.GetString("parameter.primaryKeyInfo")
	if primaryKeyInfoStr != "" {
		if err := json.Unmarshal([]byte(primaryKeyInfoStr), &task.primaryKeyInfo); err != nil {
			logger.Component().WithComponent("ElasticSearchWriter").Warn("Failed to parse primaryKeyInfo configuration",
				zap.Error(err))
		} else {
			task.hasPrimaryKeyInfo = true
			// 构建列名到索引的映射
			task.buildColumnIndexMap()
		}
	}

	// 获取分区列信息
	esPartitionColumnStr := cfg.GetString("parameter.esPartitionColumn")
	if esPartitionColumnStr != "" {
		if err := json.Unmarshal([]byte(esPartitionColumnStr), &task.esPartitionColumn); err != nil {
			logger.Component().WithComponent("ElasticSearchWriter").Warn("Failed to parse esPartitionColumn configuration",
				zap.Error(err))
		} else {
			task.hasEsPartitionColumn = true
			// 构建列名到索引的映射
			task.buildColumnIndexMap()
		}
	}

	// 验证UPDATE模式必须有ID或主键信息
	if task.actionType == ActionTypeUpdate && !task.hasID && !task.hasPrimaryKeyInfo {
		return fmt.Errorf("update mode must specify column type with id or primaryKeyInfo config")
	}

	// 创建ElasticSearch客户端
	addresses := strings.Split(task.endpoint, ",")

	clientConfig := elasticsearch.Config{
		Addresses: addresses,
	}

	if task.username != "" && task.password != "" {
		clientConfig.Username = task.username
		clientConfig.Password = task.password
	}

	var err error
	task.client, err = elasticsearch.NewClient(clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	return nil
}

func (task *ElasticSearchWriterTask) buildColumnIndexMap() {
	// 为主键列构建索引映射
	if task.hasPrimaryKeyInfo && task.primaryKeyInfo != nil {
		for _, pkCol := range task.primaryKeyInfo.Column {
			for i, col := range task.columns {
				if col.Name == pkCol {
					task.colNameToIndexMap[pkCol] = i
					break
				}
			}
		}
	}

	// 为分区列构建索引映射
	if task.hasEsPartitionColumn {
		for _, partCol := range task.esPartitionColumn {
			for i, col := range task.columns {
				if col.Name == partCol.Name {
					task.colNameToIndexMap[partCol.Name] = i
					break
				}
			}
		}
	}
}

func (task *ElasticSearchWriterTask) Prepare() error {
	return nil
}

func (task *ElasticSearchWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	batch := make([]element.Record, 0, task.batchSize)

	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == coreplugin.ErrChannelClosed {
				break
			}
			return err
		}

		batch = append(batch, record)
		if len(batch) >= task.batchSize {
			if err := task.doBatchWrite(batch); err != nil {
				if !task.ignoreWriteError {
					return err
				}
				logger.Component().WithComponent("ElasticSearchWriter").Warn("Batch write failed, but ignoring error",
					zap.Error(err))
			}
			batch = batch[:0] // 清空slice但保留容量
		}
	}

	// 处理剩余记录
	if len(batch) > 0 {
		if err := task.doBatchWrite(batch); err != nil {
			if !task.ignoreWriteError {
				return err
			}
			logger.Component().WithComponent("ElasticSearchWriter").Warn("Final batch write failed, but ignoring error",
				zap.Error(err))
		}
	}

	return nil
}

func (task *ElasticSearchWriterTask) doBatchWrite(records []element.Record) error {
	if len(records) == 0 {
		return nil
	}

	// 构建bulk请求体
	var buf bytes.Buffer

	for _, record := range records {
		doc, id, routing, version, isDelete, err := task.processRecord(record)
		if err != nil {
			if task.ignoreParseError {
				logger.Component().WithComponent("ElasticSearchWriter").Warn("Failed to process record, skipping",
					zap.Error(err))
				continue
			}
			return fmt.Errorf("failed to process record: %w", err)
		}

		// 构建操作元数据
		var action map[string]interface{}
		var actionName string

		if isDelete {
			actionName = "delete"
			action = map[string]interface{}{
				"delete": map[string]interface{}{
					"_index": task.index,
					"_id":    id,
				},
			}
			if task.esVersion < 7 {
				action["delete"].(map[string]interface{})["_type"] = task.indexType
			}
		} else {
			switch task.actionType {
			case ActionTypeIndex:
				actionName = "index"
				action = map[string]interface{}{
					"index": map[string]interface{}{
						"_index": task.index,
					},
				}
				if id != "" {
					action["index"].(map[string]interface{})["_id"] = id
				}
				if task.esVersion < 7 {
					action["index"].(map[string]interface{})["_type"] = task.indexType
				}
			case ActionTypeCreate:
				actionName = "create"
				action = map[string]interface{}{
					"create": map[string]interface{}{
						"_index": task.index,
					},
				}
				if id != "" {
					action["create"].(map[string]interface{})["_id"] = id
				}
				if task.esVersion < 7 {
					action["create"].(map[string]interface{})["_type"] = task.indexType
				}
			case ActionTypeUpdate:
				actionName = "update"
				action = map[string]interface{}{
					"update": map[string]interface{}{
						"_index": task.index,
						"_id":    id,
					},
				}
				if task.esVersion < 7 {
					action["update"].(map[string]interface{})["_type"] = task.indexType
				}
				// 对于update操作，需要包装doc
				doc = map[string]interface{}{
					"doc":           doc,
					"doc_as_upsert": true,
				}
			}

			// 添加routing和version
			if routing != "" {
				action[actionName].(map[string]interface{})["routing"] = routing
			}
			if version != "" && task.actionType != ActionTypeUpdate {
				action[actionName].(map[string]interface{})["version"] = version
				action[actionName].(map[string]interface{})["version_type"] = "external"
			}
		}

		// 写入操作元数据行
		actionBytes, err := json.Marshal(action)
		if err != nil {
			return fmt.Errorf("failed to marshal action: %w", err)
		}
		buf.Write(actionBytes)
		buf.WriteByte('\n')

		// 写入文档内容行（delete操作不需要文档内容）
		if !isDelete {
			var docBytes []byte
			if task.enableWriteNull {
				docBytes, err = json.Marshal(doc)
			} else {
				docBytes, err = task.marshalWithoutNull(doc)
			}
			if err != nil {
				return fmt.Errorf("failed to marshal document: %w", err)
			}
			buf.Write(docBytes)
			buf.WriteByte('\n')
		}
	}

	// 执行bulk请求
	return task.executeBulkRequest(&buf)
}

func (task *ElasticSearchWriterTask) processRecord(record element.Record) (map[string]interface{}, string, string, string, bool, error) {
	doc := make(map[string]interface{})
	var id, routing, version string

	// 检查是否为删除记录
	isDelete := task.isDeleteRecord(record)
	if isDelete {
		// 对于删除操作，只需要获取ID
		if task.hasID || task.hasPrimaryKeyInfo {
			var err error
			id, err = task.extractID(record)
			if err != nil {
				return nil, "", "", "", false, err
			}
		}
		return nil, id, "", "", true, nil
	}

	// 处理常规记录
	columnCount := record.GetColumnNumber()
	if columnCount != len(task.columns) {
		return nil, "", "", "", false, fmt.Errorf("column count mismatch: expected %d, got %d", len(task.columns), columnCount)
	}

	for i := 0; i < columnCount; i++ {
		column := record.GetColumn(i)
		colConfig := task.columns[i]
		fieldType := ESFieldType(colConfig.Type)

		// 跳过组合ID字段中的组成字段
		if task.combinedIdColumn != nil && task.combinedIdColumn.CombineFields != nil {
			skip := false
			for _, combineField := range task.combinedIdColumn.CombineFields {
				if colConfig.Name == combineField {
					skip = true
					break
				}
			}
			if skip {
				continue
			}
		}

		switch fieldType {
		case FieldTypeID:
			if id != "" {
				id += column.GetAsString()
			} else {
				id = column.GetAsString()
			}
		case FieldTypeVersion:
			if version != "" {
				version += column.GetAsString()
			} else {
				version = column.GetAsString()
			}
		case FieldTypeRouting:
			if routing != "" {
				routing += column.GetAsString()
			} else {
				routing = column.GetAsString()
			}
		case FieldTypeParent:
			// Parent字段在ES 6.x+中已废弃，这里暂时跳过
			continue
		default:
			// 处理普通字段
			value, err := task.convertColumnValue(column, colConfig)
			if err != nil {
				return nil, "", "", "", false, fmt.Errorf("failed to convert column %s: %w", colConfig.Name, err)
			}
			doc[colConfig.Name] = value
		}
	}

	// 处理组合ID
	if task.combinedIdColumn != nil && len(task.combinedIdColumn.CombineFields) > 0 {
		var err error
		id, err = task.processCombinedID(record, *task.combinedIdColumn)
		if err != nil {
			return nil, "", "", "", false, err
		}
	}

	// 处理主键信息
	if task.hasPrimaryKeyInfo && task.primaryKeyInfo != nil {
		var err error
		id, err = task.extractPrimaryKeyID(record)
		if err != nil {
			return nil, "", "", "", false, err
		}
	}

	// 处理分区列
	if task.hasEsPartitionColumn {
		var err error
		routing, err = task.extractPartitionRouting(record)
		if err != nil {
			return nil, "", "", "", false, err
		}
	}

	return doc, id, routing, version, false, nil
}

func (task *ElasticSearchWriterTask) convertColumnValue(column element.Column, colConfig ESColumn) (interface{}, error) {
	fieldType := ESFieldType(colConfig.Type)

	// 处理null值
	if column.IsNull() {
		return nil, nil
	}

	// 处理数组类型
	if colConfig.Array {
		arrayStr := column.GetAsString()
		if arrayStr == "" {
			return nil, nil
		}

		items := strings.Split(arrayStr, task.splitter)
		if !colConfig.DstArray {
			return items, nil
		}

		// 根据目标类型转换数组元素
		switch fieldType {
		case FieldTypeInteger, FieldTypeShort:
			result := make([]int, len(items))
			for i, item := range items {
				if strings.TrimSpace(item) == "" {
					continue
				}
				val, err := strconv.Atoi(strings.TrimSpace(item))
				if err != nil {
					return nil, err
				}
				result[i] = val
			}
			return result, nil
		case FieldTypeLong:
			result := make([]int64, len(items))
			for i, item := range items {
				if strings.TrimSpace(item) == "" {
					continue
				}
				val, err := strconv.ParseInt(strings.TrimSpace(item), 10, 64)
				if err != nil {
					return nil, err
				}
				result[i] = val
			}
			return result, nil
		case FieldTypeFloat, FieldTypeDouble:
			result := make([]float64, len(items))
			for i, item := range items {
				if strings.TrimSpace(item) == "" {
					continue
				}
				val, err := strconv.ParseFloat(strings.TrimSpace(item), 64)
				if err != nil {
					return nil, err
				}
				result[i] = val
			}
			return result, nil
		default:
			return items, nil
		}
	}

	// 处理单值类型
	switch fieldType {
	case FieldTypeString, FieldTypeKeyword, FieldTypeText, FieldTypeIP, FieldTypeGeoPoint, FieldTypeIPRange:
		return column.GetAsString(), nil
	case FieldTypeBoolean:
		boolVal, err := column.GetAsBool()
		if err != nil {
			return nil, err
		}
		return boolVal, nil
	case FieldTypeByte, FieldTypeBinary:
		return column.GetAsString(), nil
	case FieldTypeLong:
		longVal, err := column.GetAsLong()
		if err != nil {
			return nil, err
		}
		return longVal, nil
	case FieldTypeInteger, FieldTypeShort:
		longVal, err := column.GetAsLong()
		if err != nil {
			return nil, err
		}
		return longVal, nil
	case FieldTypeFloat, FieldTypeDouble:
		floatVal, err := column.GetAsDouble()
		if err != nil {
			return nil, err
		}
		return floatVal, nil
	case FieldTypeDate:
		return task.convertDateField(column, colConfig)
	case FieldTypeGeoShape, FieldTypeDateRange, FieldTypeIntegerRange,
		 FieldTypeFloatRange, FieldTypeLongRange, FieldTypeDoubleRange:
		// 这些类型需要JSON解析
		str := column.GetAsString()
		if str == "" {
			return nil, nil
		}
		var result interface{}
		if err := json.Unmarshal([]byte(str), &result); err != nil {
			return nil, err
		}
		return result, nil
	case FieldTypeNested, FieldTypeObject:
		// 对象类型需要JSON解析
		str := column.GetAsString()
		if str == "" {
			return nil, nil
		}
		if colConfig.JsonArray {
			// 作为嵌套对象处理
			var result interface{}
			if err := json.Unmarshal([]byte(str), &result); err != nil {
				return nil, err
			}
			return result, nil
		} else {
			var result interface{}
			if err := json.Unmarshal([]byte(str), &result); err != nil {
				return nil, err
			}
			return result, nil
		}
	default:
		return column.GetAsString(), nil
	}
}

func (task *ElasticSearchWriterTask) convertDateField(column element.Column, colConfig ESColumn) (interface{}, error) {
	// 如果保持原样，直接返回
	if colConfig.Origin {
		return column.GetAsString(), nil
	}

	// 处理时间转换逻辑
	if column.GetType() == element.TypeDate {
		// 如果是时间类型，转换为时间戳
		dateVal, err := column.GetAsDate()
		if err != nil {
			return nil, err
		}
		return dateVal, nil
	}

	// 如果有格式化配置，需要解析时间字符串
	if colConfig.Format != "" {
		timeStr := column.GetAsString()
		if timeStr == "" {
			return nil, nil
		}
		// 这里可以根据format配置解析时间
		// 暂时返回原始字符串
		return timeStr, nil
	}

	return column.GetAsString(), nil
}

func (task *ElasticSearchWriterTask) isDeleteRecord(record element.Record) bool {
	if len(task.deleteBy) == 0 {
		return false
	}

	// 构建记录的键值映射
	recordMap := make(map[string]string)
	columnCount := record.GetColumnNumber()
	for i := 0; i < columnCount && i < len(task.columns); i++ {
		column := record.GetColumn(i)
		recordMap[task.columns[i].Name] = column.GetAsString()
	}

	// 检查是否满足删除条件
	for _, condition := range task.deleteBy {
		if task.meetAllConditions(recordMap, condition) {
			return true
		}
	}

	return false
}

func (task *ElasticSearchWriterTask) meetAllConditions(recordMap map[string]string, condition map[string]interface{}) bool {
	for key, expectedValue := range condition {
		recordValue, exists := recordMap[key]
		if !exists {
			return false
		}

		// 检查单个条件
		if !task.checkCondition(recordValue, expectedValue) {
			return false
		}
	}
	return true
}

func (task *ElasticSearchWriterTask) checkCondition(recordValue string, expectedValue interface{}) bool {
	switch expected := expectedValue.(type) {
	case []interface{}:
		// 数组条件：recordValue在数组中的任一个值匹配即可
		for _, item := range expected {
			if recordValue == fmt.Sprintf("%v", item) {
				return true
			}
		}
		return false
	default:
		// 单值条件
		return recordValue == fmt.Sprintf("%v", expected)
	}
}

func (task *ElasticSearchWriterTask) extractID(record element.Record) (string, error) {
	if task.combinedIdColumn != nil {
		return task.processCombinedID(record, *task.combinedIdColumn)
	}

	// 查找ID字段
	columnCount := record.GetColumnNumber()
	for i := 0; i < columnCount && i < len(task.columns); i++ {
		if ESFieldType(task.columns[i].Type) == FieldTypeID {
			column := record.GetColumn(i)
			return column.GetAsString(), nil
		}
	}

	return "", fmt.Errorf("no id field found")
}

func (task *ElasticSearchWriterTask) processCombinedID(record element.Record, idColumn ESColumn) (string, error) {
	var values []string

	for _, fieldName := range idColumn.CombineFields {
		// 查找字段索引
		fieldIndex := -1
		for i, col := range task.columns {
			if col.Name == fieldName {
				fieldIndex = i
				break
			}
		}

		if fieldIndex == -1 {
			return "", fmt.Errorf("combine field %s not found", fieldName)
		}

		if fieldIndex >= record.GetColumnNumber() {
			return "", fmt.Errorf("field index %d out of range", fieldIndex)
		}

		column := record.GetColumn(fieldIndex)
		values = append(values, column.GetAsString())
	}

	return strings.Join(values, idColumn.CombineFieldsValueSeparator), nil
}

func (task *ElasticSearchWriterTask) extractPrimaryKeyID(record element.Record) (string, error) {
	if task.primaryKeyInfo == nil {
		return "", fmt.Errorf("primary key info not configured")
	}

	var values []string
	for _, pkCol := range task.primaryKeyInfo.Column {
		colIndex, exists := task.colNameToIndexMap[pkCol]
		if !exists {
			return "", fmt.Errorf("primary key column %s not found", pkCol)
		}

		if colIndex >= record.GetColumnNumber() {
			return "", fmt.Errorf("column index %d out of range", colIndex)
		}

		column := record.GetColumn(colIndex)
		values = append(values, column.GetAsString())
	}

	delimiter := task.primaryKeyInfo.FieldDelimiter
	if delimiter == "" {
		delimiter = task.fieldDelimiter
	}

	return strings.Join(values, delimiter), nil
}

func (task *ElasticSearchWriterTask) extractPartitionRouting(record element.Record) (string, error) {
	var values []string
	for _, partCol := range task.esPartitionColumn {
		colIndex, exists := task.colNameToIndexMap[partCol.Name]
		if !exists {
			return "", fmt.Errorf("partition column %s not found", partCol.Name)
		}

		if colIndex >= record.GetColumnNumber() {
			return "", fmt.Errorf("column index %d out of range", colIndex)
		}

		column := record.GetColumn(colIndex)
		values = append(values, column.GetAsString())
	}

	return strings.Join(values, ""), nil
}

func (task *ElasticSearchWriterTask) marshalWithoutNull(v interface{}) ([]byte, error) {
	// 简单实现：先正常marshal，然后移除null字段
	// 这个实现可能不够高效，但功能正确
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	// 如果不需要处理null值，直接返回
	if task.enableWriteNull {
		return data, nil
	}

	// 解析为map，移除null值，再重新marshal
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return data, nil // 如果解析失败，返回原始数据
	}

	// 递归移除null值
	task.removeNullValues(m)

	return json.Marshal(m)
}

func (task *ElasticSearchWriterTask) removeNullValues(m map[string]interface{}) {
	for key, value := range m {
		if value == nil {
			delete(m, key)
		} else if subMap, ok := value.(map[string]interface{}); ok {
			task.removeNullValues(subMap)
		}
	}
}

func (task *ElasticSearchWriterTask) executeBulkRequest(buf *bytes.Buffer) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(task.timeout)*time.Second)
	defer cancel()

	req := esapi.BulkRequest{
		Body: buf,
	}

	// 添加URL参数
	if task.urlParams != nil {
		// 这里需要根据esapi.BulkRequest的具体字段来设置参数
		// 暂时简化处理
	}

	res, err := req.Do(ctx, task.client)
	if err != nil {
		return fmt.Errorf("bulk request failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk request error: %s", res.String())
	}

	// 解析响应检查是否有错误
	var bulkResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&bulkResponse); err != nil {
		return fmt.Errorf("failed to decode bulk response: %w", err)
	}

	// 检查是否有错误项
	if errors, ok := bulkResponse["errors"].(bool); ok && errors {
		items, ok := bulkResponse["items"].([]interface{})
		if ok {
			for _, item := range items {
				if itemMap, ok := item.(map[string]interface{}); ok {
					for _, actionMap := range itemMap {
						if action, ok := actionMap.(map[string]interface{}); ok {
							if errorInfo, hasError := action["error"]; hasError {
								// 记录错误但不抛出异常（如果配置了忽略错误）
								logger.Component().WithComponent("ElasticSearchWriter").Warn("Bulk item error",
									zap.Any("error", errorInfo))

								if !task.ignoreParseError {
									return fmt.Errorf("bulk item error: %v", errorInfo)
								}
							}
						}
					}
				}
			}
		}
	}

	return nil
}

func (task *ElasticSearchWriterTask) Post() error {
	return nil
}

func (task *ElasticSearchWriterTask) Destroy() error {
	// 关闭资源
	return nil
}