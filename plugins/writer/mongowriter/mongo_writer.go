package mongowriter

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
	coreplugin "github.com/longkeyy/go-datax/core/registry"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DefaultBatchSize = 1000
)

// MongoWriterJob MongoDB写入作业
type MongoWriterJob struct {
	config         config.Configuration
	addresses      []string
	userName       string
	userPassword   string
	dbName         string
	collectionName string
	columns        []map[string]interface{}
	preSql         []string
	writeMode      map[string]interface{}
	batchSize      int
	factory        *factory.DataXFactory
}

func NewMongoWriterJob() *MongoWriterJob {
	return &MongoWriterJob{
		batchSize: DefaultBatchSize,
		writeMode: map[string]interface{}{
			"isReplace": "false",
		},
		factory: factory.GetGlobalFactory(),
	}
}

func (job *MongoWriterJob) Init(config config.Configuration) error {
	job.config = config

	// 获取必需参数
	addresses := config.GetList("parameter.address")
	if len(addresses) == 0 {
		return fmt.Errorf("address is required")
	}

	for _, addr := range addresses {
		if addrStr, ok := addr.(string); ok {
			job.addresses = append(job.addresses, addrStr)
		}
	}

	job.userName = config.GetString("parameter.userName")
	if job.userName == "" {
		return fmt.Errorf("userName is required")
	}

	job.userPassword = config.GetString("parameter.userPassword")
	if job.userPassword == "" {
		return fmt.Errorf("userPassword is required")
	}

	job.dbName = config.GetString("parameter.dbName")
	if job.dbName == "" {
		return fmt.Errorf("dbName is required")
	}

	job.collectionName = config.GetString("parameter.collectionName")
	if job.collectionName == "" {
		return fmt.Errorf("collectionName is required")
	}

	// 获取列配置
	columnsConfig := config.GetList("parameter.column")
	if len(columnsConfig) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	for _, columnConfig := range columnsConfig {
		if column, ok := columnConfig.(map[string]interface{}); ok {
			// 设置默认类型为string
			if _, exists := column["type"]; !exists {
				column["type"] = "string"
			}
			job.columns = append(job.columns, column)
		}
	}

	if len(job.columns) == 0 {
		return fmt.Errorf("no valid columns configured")
	}

	// 获取可选参数
	job.preSql = config.GetStringList("parameter.preSql")

	// 获取writeMode配置
	if writeModeConfig := config.GetMap("parameter.writeMode"); writeModeConfig != nil {
		job.writeMode = writeModeConfig
	}

	job.batchSize = config.GetIntWithDefault("parameter.batchSize", DefaultBatchSize)

	log.Printf("MongoWriter initialized: addresses=%v, db=%s, collection=%s, columns=%d",
		job.addresses, job.dbName, job.collectionName, len(job.columns))
	return nil
}

func (job *MongoWriterJob) Prepare() error {
	// 执行pre SQL语句
	if len(job.preSql) > 0 {
		client, err := job.createMongoClient()
		if err != nil {
			return fmt.Errorf("failed to create MongoDB client: %v", err)
		}
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			client.Disconnect(ctx)
		}()

		collection := client.Database(job.dbName).Collection(job.collectionName)
		ctx := context.Background()

		for _, sql := range job.preSql {
			log.Printf("Executing preSql: %s", sql)
			switch strings.ToLower(strings.TrimSpace(sql)) {
			case "drop":
				err := collection.Drop(ctx)
				if err != nil {
					log.Printf("Warning: failed to drop collection: %v", err)
				}
			case "remove":
				_, err := collection.DeleteMany(ctx, bson.M{})
				if err != nil {
					log.Printf("Warning: failed to remove documents: %v", err)
				}
			default:
				log.Printf("Unsupported preSql command: %s", sql)
			}
		}
	}

	return nil
}

func (job *MongoWriterJob) createMongoClient() (*mongo.Client, error) {
	// 构建连接URI
	uri := fmt.Sprintf("mongodb://%s:%s@%s/%s",
		job.userName, job.userPassword,
		strings.Join(job.addresses, ","), job.dbName)

	clientOptions := options.Client().ApplyURI(uri)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	// 测试连接
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (job *MongoWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	taskConfigs := make([]config.Configuration, 0)

	// 创建指定数量的任务配置
	for i := 0; i < mandatoryNumber; i++ {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	log.Printf("Split into %d tasks", len(taskConfigs))
	return taskConfigs, nil
}

func (job *MongoWriterJob) Post() error {
	return nil
}

func (job *MongoWriterJob) Destroy() error {
	return nil
}

// MongoWriterTask MongoDB写入任务
type MongoWriterTask struct {
	config    config.Configuration
	writerJob *MongoWriterJob
	client    *mongo.Client
	buffer    []bson.M
	factory   *factory.DataXFactory
}

func NewMongoWriterTask() *MongoWriterTask {
	return &MongoWriterTask{
		buffer:  make([]bson.M, 0),
		factory: factory.GetGlobalFactory(),
	}
}

func (task *MongoWriterTask) Init(config config.Configuration) error {
	task.config = config

	// 创建WriterJob来重用配置逻辑
	task.writerJob = NewMongoWriterJob()
	err := task.writerJob.Init(config)
	if err != nil {
		return err
	}

	// 创建MongoDB连接
	task.client, err = task.writerJob.createMongoClient()
	if err != nil {
		return fmt.Errorf("failed to create MongoDB client: %v", err)
	}

	log.Printf("MongoDB connection established for writing")
	return nil
}

func (task *MongoWriterTask) Prepare() error {
	return nil
}

func (task *MongoWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	defer func() {
		// 写入剩余的记录
		if len(task.buffer) > 0 {
			task.flushBuffer()
		}
		// 关闭连接
		if task.client != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			task.client.Disconnect(ctx)
		}
	}()

	recordCount := int64(0)

	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == coreplugin.ErrChannelClosed {
				break
			}
			return fmt.Errorf("failed to receive record: %v", err)
		}

		// 转换记录为MongoDB文档
		doc := task.recordToDocument(record)
		task.buffer = append(task.buffer, doc)

		// 当缓冲区满时，批量写入
		if len(task.buffer) >= task.writerJob.batchSize {
			if err := task.flushBuffer(); err != nil {
				return err
			}
		}

		recordCount++

		// 每1000条记录输出一次进度
		if recordCount%1000 == 0 {
			log.Printf("Processed %d records", recordCount)
		}
	}

	log.Printf("Total records processed: %d", recordCount)
	return nil
}

func (task *MongoWriterTask) recordToDocument(record element.Record) bson.M {
	doc := bson.M{}
	columnCount := record.GetColumnNumber()

	for i := 0; i < columnCount && i < len(task.writerJob.columns); i++ {
		columnConfig := task.writerJob.columns[i]
		fieldName, _ := columnConfig["name"].(string)
		fieldType, _ := columnConfig["type"].(string)
		splitter, _ := columnConfig["splitter"].(string)
		itemType, _ := columnConfig["itemtype"].(string)

		column := record.GetColumn(i)
		if column == nil {
			doc[fieldName] = nil
			continue
		}

		value := task.convertColumnValue(column, fieldType, splitter, itemType)
		doc[fieldName] = value
	}

	return doc
}

func (task *MongoWriterTask) convertColumnValue(column element.Column, fieldType, splitter, itemType string) interface{} {
	if column == nil {
		return nil
	}

	switch fieldType {
	case "ObjectId":
		strVal := column.GetAsString()
		if oid, err := primitive.ObjectIDFromHex(strVal); err == nil {
			return oid
		}
		return strVal

	case "long":
		if val, err := column.GetAsLong(); err == nil {
			return val
		}
		return int64(0)

	case "double":
		if val, err := column.GetAsDouble(); err == nil {
			return val
		}
		return float64(0)

	case "boolean", "bool":
		if val, err := column.GetAsBool(); err == nil {
			return val
		}
		return false

	case "date":
		if val, err := column.GetAsDate(); err == nil {
			return primitive.NewDateTimeFromTime(val)
		}
		return primitive.NewDateTimeFromTime(time.Now())

	case "bytes":
		if val, err := column.GetAsBytes(); err == nil {
			return primitive.Binary{Data: val}
		}
		return primitive.Binary{Data: []byte{}}

	case "array":
		strVal := column.GetAsString()
		if splitter == "" {
			splitter = ","
		}
		items := strings.Split(strVal, splitter)

		// 根据itemtype转换数组元素
		var result primitive.A
		for _, item := range items {
			item = strings.TrimSpace(item)
			switch itemType {
			case "long":
				if intVal, err := strconv.ParseInt(item, 10, 64); err == nil {
					result = append(result, intVal)
				} else {
					result = append(result, item)
				}
			case "double":
				if floatVal, err := strconv.ParseFloat(item, 64); err == nil {
					result = append(result, floatVal)
				} else {
					result = append(result, item)
				}
			case "boolean", "bool":
				if boolVal, err := strconv.ParseBool(item); err == nil {
					result = append(result, boolVal)
				} else {
					result = append(result, item)
				}
			default: // string
				result = append(result, item)
			}
		}
		return result

	default: // string
		return column.GetAsString()
	}
}

func (task *MongoWriterTask) flushBuffer() error {
	if len(task.buffer) == 0 {
		return nil
	}

	collection := task.client.Database(task.writerJob.dbName).Collection(task.writerJob.collectionName)
	ctx := context.Background()

	// 检查写入模式
	isReplace, _ := task.writerJob.writeMode["isReplace"].(string)
	if isReplace == "true" {
		// Replace模式：使用upsert
		replaceKey, _ := task.writerJob.writeMode["replaceKey"].(string)
		if replaceKey == "" {
			return fmt.Errorf("replaceKey is required when isReplace is true")
		}

		var operations []mongo.WriteModel
		for _, doc := range task.buffer {
			filter := bson.M{replaceKey: doc[replaceKey]}
			operation := mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(doc).SetUpsert(true)
			operations = append(operations, operation)
		}

		_, err := collection.BulkWrite(ctx, operations)
		if err != nil {
			return fmt.Errorf("failed to execute bulk replace: %v", err)
		}
	} else {
		// Insert模式：直接插入
		var documents []interface{}
		for _, doc := range task.buffer {
			documents = append(documents, doc)
		}

		_, err := collection.InsertMany(ctx, documents)
		if err != nil {
			return fmt.Errorf("failed to insert documents: %v", err)
		}
	}

	log.Printf("Flushed %d documents to MongoDB", len(task.buffer))
	task.buffer = task.buffer[:0] // 清空缓冲区
	return nil
}

func (task *MongoWriterTask) Post() error {
	return nil
}

func (task *MongoWriterTask) Destroy() error {
	if task.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		task.client.Disconnect(ctx)
	}
	return nil
}