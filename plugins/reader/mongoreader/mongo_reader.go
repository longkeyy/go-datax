package mongoreader

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoReaderJob MongoDB读取作业
type MongoReaderJob struct {
	config         *config.Configuration
	addresses      []string
	userName       string
	userPassword   string
	dbName         string
	authDb         string
	collectionName string
	columns        []map[string]interface{}
	query          string
	batchSize      int64
}

func NewMongoReaderJob() *MongoReaderJob {
	return &MongoReaderJob{
		batchSize: 1000,
	}
}

func (job *MongoReaderJob) Init(config *config.Configuration) error {
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

	job.dbName = config.GetString("parameter.dbName")
	if job.dbName == "" {
		return fmt.Errorf("dbName is required")
	}

	job.collectionName = config.GetString("parameter.collectionName")
	if job.collectionName == "" {
		return fmt.Errorf("collectionName is required")
	}

	// 获取可选参数
	job.userName = config.GetString("parameter.userName")
	job.userPassword = config.GetString("parameter.userPassword")
	job.authDb = config.GetStringWithDefault("parameter.authDb", job.dbName)
	job.query = config.GetString("parameter.query")
	job.batchSize = int64(config.GetIntWithDefault("parameter.batchSize", 1000))

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

	log.Printf("MongoReader initialized: addresses=%v, db=%s, collection=%s, columns=%d",
		job.addresses, job.dbName, job.collectionName, len(job.columns))
	return nil
}

func (job *MongoReaderJob) Prepare() error {
	return nil
}

func (job *MongoReaderJob) Split(adviceNumber int) ([]*config.Configuration, error) {
	taskConfigs := make([]*config.Configuration, 0)

	// 创建指定数量的任务配置
	for i := 0; i < adviceNumber; i++ {
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", i)
		taskConfigs = append(taskConfigs, taskConfig)
	}

	log.Printf("Split into %d tasks", len(taskConfigs))
	return taskConfigs, nil
}

func (job *MongoReaderJob) Post() error {
	return nil
}

func (job *MongoReaderJob) Destroy() error {
	return nil
}

// MongoReaderTask MongoDB读取任务
type MongoReaderTask struct {
	config    *config.Configuration
	readerJob *MongoReaderJob
	client    *mongo.Client
}

func NewMongoReaderTask() *MongoReaderTask {
	return &MongoReaderTask{}
}

func (task *MongoReaderTask) Init(config *config.Configuration) error {
	task.config = config

	// 创建ReaderJob来重用配置逻辑
	task.readerJob = NewMongoReaderJob()
	err := task.readerJob.Init(config)
	if err != nil {
		return err
	}

	// 创建MongoDB连接
	task.client, err = task.createMongoClient()
	if err != nil {
		return fmt.Errorf("failed to create MongoDB client: %v", err)
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = task.client.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to ping MongoDB: %v", err)
	}

	log.Printf("MongoDB connection established")
	return nil
}

func (task *MongoReaderTask) createMongoClient() (*mongo.Client, error) {
	// 构建连接URI
	uri := "mongodb://"
	if task.readerJob.userName != "" && task.readerJob.userPassword != "" {
		uri += fmt.Sprintf("%s:%s@", task.readerJob.userName, task.readerJob.userPassword)
	}
	uri += strings.Join(task.readerJob.addresses, ",")
	uri += "/" + task.readerJob.authDb

	clientOptions := options.Client().ApplyURI(uri)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (task *MongoReaderTask) Prepare() error {
	return nil
}

func (task *MongoReaderTask) StartRead(recordSender plugin.RecordSender) error {
	collection := task.client.Database(task.readerJob.dbName).Collection(task.readerJob.collectionName)

	// 构建查询条件
	filter := bson.M{}
	if task.readerJob.query != "" {
		err := json.Unmarshal([]byte(task.readerJob.query), &filter)
		if err != nil {
			return fmt.Errorf("failed to parse query: %v", err)
		}
	}

	// 构建投影（选择字段）
	projection := bson.M{}
	for _, column := range task.readerJob.columns {
		if name, ok := column["name"].(string); ok {
			projection[name] = 1
		}
	}

	// 设置查询选项
	findOptions := options.Find()
	findOptions.SetProjection(projection)
	findOptions.SetBatchSize(int32(task.readerJob.batchSize))

	log.Printf("Starting MongoDB query with filter: %+v", filter)

	ctx := context.Background()
	cursor, err := collection.Find(ctx, filter, findOptions)
	if err != nil {
		return fmt.Errorf("failed to execute MongoDB query: %v", err)
	}
	defer cursor.Close(ctx)

	recordCount := int64(0)
	for cursor.Next(ctx) {
		var doc bson.M
		err := cursor.Decode(&doc)
		if err != nil {
			return fmt.Errorf("failed to decode document: %v", err)
		}

		// 转换文档为DataX记录
		record := element.NewRecord()
		for _, columnConfig := range task.readerJob.columns {
			column := task.convertDocumentField(doc, columnConfig)
			record.AddColumn(column)
		}

		// 发送记录
		if err := recordSender.SendRecord(record); err != nil {
			return fmt.Errorf("failed to send record: %v", err)
		}

		recordCount++

		// 每1000条记录输出一次进度
		if recordCount%1000 == 0 {
			log.Printf("Read %d records", recordCount)
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error: %v", err)
	}

	log.Printf("Completed reading %d records", recordCount)
	return nil
}

func (task *MongoReaderTask) convertDocumentField(doc bson.M, columnConfig map[string]interface{}) element.Column {
	fieldName, _ := columnConfig["name"].(string)
	fieldType, _ := columnConfig["type"].(string)
	splitter, _ := columnConfig["splitter"].(string)

	value, exists := doc[fieldName]
	if !exists || value == nil {
		return element.NewStringColumn("")
	}

	switch fieldType {
	case "ObjectId":
		if oid, ok := value.(primitive.ObjectID); ok {
			return element.NewStringColumn(oid.Hex())
		}
		return element.NewStringColumn(fmt.Sprintf("%v", value))

	case "long":
		switch v := value.(type) {
		case int32:
			return element.NewLongColumn(int64(v))
		case int64:
			return element.NewLongColumn(v)
		case float64:
			return element.NewLongColumn(int64(v))
		case string:
			if intVal, err := strconv.ParseInt(v, 10, 64); err == nil {
				return element.NewLongColumn(intVal)
			}
		}
		return element.NewLongColumn(0)

	case "double":
		switch v := value.(type) {
		case float32:
			return element.NewDoubleColumn(float64(v))
		case float64:
			return element.NewDoubleColumn(v)
		case int32:
			return element.NewDoubleColumn(float64(v))
		case int64:
			return element.NewDoubleColumn(float64(v))
		case string:
			if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
				return element.NewDoubleColumn(floatVal)
			}
		}
		return element.NewDoubleColumn(0.0)

	case "boolean", "bool":
		if boolVal, ok := value.(bool); ok {
			return element.NewBoolColumn(boolVal)
		}
		if strVal, ok := value.(string); ok {
			if boolVal, err := strconv.ParseBool(strVal); err == nil {
				return element.NewBoolColumn(boolVal)
			}
		}
		return element.NewBoolColumn(false)

	case "date":
		if dateVal, ok := value.(primitive.DateTime); ok {
			return element.NewDateColumn(dateVal.Time())
		}
		if timeVal, ok := value.(time.Time); ok {
			return element.NewDateColumn(timeVal)
		}
		if strVal, ok := value.(string); ok {
			if dateVal, err := time.Parse(time.RFC3339, strVal); err == nil {
				return element.NewDateColumn(dateVal)
			}
		}
		return element.NewDateColumn(time.Now())

	case "bytes":
		if bytesVal, ok := value.(primitive.Binary); ok {
			return element.NewBytesColumn(bytesVal.Data)
		}
		if strVal, ok := value.(string); ok {
			return element.NewBytesColumn([]byte(strVal))
		}
		return element.NewBytesColumn([]byte{})

	case "array":
		if arrayVal, ok := value.(primitive.A); ok {
			var strValues []string
			for _, item := range arrayVal {
				strValues = append(strValues, fmt.Sprintf("%v", item))
			}
			if splitter == "" {
				splitter = ","
			}
			return element.NewStringColumn(strings.Join(strValues, splitter))
		}
		return element.NewStringColumn("")

	default: // string
		return element.NewStringColumn(fmt.Sprintf("%v", value))
	}
}

func (task *MongoReaderTask) Post() error {
	return nil
}

func (task *MongoReaderTask) Destroy() error {
	if task.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		task.client.Disconnect(ctx)
	}
	return nil
}