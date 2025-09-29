package dorisreader

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/factory"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// DorisReaderJob Doris读取作业
type DorisReaderJob struct {
	config     config.Configuration
	username   string
	password   string
	jdbcUrls   []string
	tables     []string
	columns    []string
	splitPk    string
	where      string
	querySql   string
}

func NewDorisReaderJob() *DorisReaderJob {
	return &DorisReaderJob{}
}

func (job *DorisReaderJob) Init(config config.Configuration) error {
	job.config = config

	// 获取必需参数
	job.username = config.GetString("parameter.username")
	if job.username == "" {
		return fmt.Errorf("username is required")
	}

	job.password = config.GetString("parameter.password")

	// 获取连接配置
	connections := config.GetList("parameter.connection")
	if len(connections) == 0 {
		return fmt.Errorf("connection configuration is required")
	}

	for _, conn := range connections {
		if connMap, ok := conn.(map[string]interface{}); ok {
			// 获取jdbcUrl
			if jdbcUrls, exists := connMap["jdbcUrl"]; exists {
				if urls, ok := jdbcUrls.([]interface{}); ok {
					for _, url := range urls {
						if urlStr, ok := url.(string); ok {
							job.jdbcUrls = append(job.jdbcUrls, urlStr)
						}
					}
				}
			}

			// 获取table
			if tables, exists := connMap["table"]; exists {
				if tableList, ok := tables.([]interface{}); ok {
					for _, table := range tableList {
						if tableStr, ok := table.(string); ok {
							job.tables = append(job.tables, tableStr)
						}
					}
				}
			}
		}
	}

	if len(job.jdbcUrls) == 0 {
		return fmt.Errorf("jdbcUrl is required")
	}

	if len(job.tables) == 0 {
		return fmt.Errorf("table is required")
	}

	// 获取列配置
	columns := config.GetList("parameter.column")
	if len(columns) == 0 {
		return fmt.Errorf("column configuration is required")
	}

	for _, column := range columns {
		if columnStr, ok := column.(string); ok {
			job.columns = append(job.columns, columnStr)
		}
	}

	if len(job.columns) == 0 {
		return fmt.Errorf("no valid columns configured")
	}

	// 获取可选参数
	job.splitPk = config.GetString("parameter.splitPk")
	job.where = config.GetString("parameter.where")
	job.querySql = config.GetString("parameter.querySql")

	log.Printf("DorisReader initialized: jdbcUrls=%v, tables=%v, columns=%v",
		job.jdbcUrls, job.tables, job.columns)
	return nil
}

func (job *DorisReaderJob) Prepare() error {
	return nil
}

func (job *DorisReaderJob) convertJdbcUrl(jdbcUrl string) string {
	// 将JDBC URL转换为MySQL连接字符串
	// jdbc:mysql://host:port/database 转换为 user:password@tcp(host:port)/database
	jdbcUrl = strings.Replace(jdbcUrl, "jdbc:mysql://", "", 1)

	parts := strings.Split(jdbcUrl, "/")
	if len(parts) != 2 {
		return fmt.Sprintf("%s:%s@tcp(%s)/", job.username, job.password, jdbcUrl)
	}

	hostPort := parts[0]
	database := parts[1]

	return fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		job.username, job.password, hostPort, database)
}

func (job *DorisReaderJob) getTableColumns(tableName string) ([]string, error) {
	if len(job.columns) == 1 && job.columns[0] == "*" {
		// 查询表的所有列
		dsn := job.convertJdbcUrl(job.jdbcUrls[0])
		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err != nil {
			return nil, fmt.Errorf("failed to connect to Doris: %v", err)
		}

		sqlDB, err := db.DB()
		if err != nil {
			return nil, err
		}
		defer sqlDB.Close()

		query := `
			SELECT column_name
			FROM information_schema.columns
			WHERE table_schema = DATABASE()
			AND table_name = ?
			ORDER BY ordinal_position`

		var columns []string
		err = db.Raw(query, tableName).Pluck("column_name", &columns).Error
		if err != nil {
			return nil, fmt.Errorf("failed to get table columns: %v", err)
		}

		if len(columns) == 0 {
			return nil, fmt.Errorf("table %s has no columns or does not exist", tableName)
		}

		return columns, nil
	}

	return job.columns, nil
}

func (job *DorisReaderJob) Split(adviceNumber int) ([]config.Configuration, error) {
	taskConfigs := make([]config.Configuration, 0)

	// 如果有splitPk，按splitPk进行数据分片
	if job.splitPk != "" && adviceNumber > 1 {
		for _, table := range job.tables {
			tableConfigs, err := job.splitByPk(table, adviceNumber)
			if err != nil {
				return nil, err
			}
			taskConfigs = append(taskConfigs, tableConfigs...)
		}
	} else {
		// 按表进行分片
		for i, table := range job.tables {
			taskConfig := job.config.Clone()
			taskConfig.Set("taskId", i)
			taskConfig.Set("currentTable", table)
			taskConfigs = append(taskConfigs, taskConfig)
		}
	}

	log.Printf("Split into %d tasks", len(taskConfigs))
	return taskConfigs, nil
}

func (job *DorisReaderJob) splitByPk(tableName string, adviceNumber int) ([]config.Configuration, error) {
	// 连接数据库获取splitPk的范围
	dsn := job.convertJdbcUrl(job.jdbcUrls[0])
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Doris: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	defer sqlDB.Close()

	// 获取splitPk的最小值和最大值
	var minMax struct {
		MinVal int64 `gorm:"column:min_val"`
		MaxVal int64 `gorm:"column:max_val"`
	}

	query := fmt.Sprintf("SELECT MIN(%s) as min_val, MAX(%s) as max_val FROM %s",
		job.splitPk, job.splitPk, tableName)
	if job.where != "" {
		query += " WHERE " + job.where
	}

	err = db.Raw(query).Scan(&minMax).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get splitPk range: %v", err)
	}

	// 计算分片范围
	taskConfigs := make([]config.Configuration, 0)
	if minMax.MaxVal <= minMax.MinVal {
		// 只有一个分片
		taskConfig := job.config.Clone()
		taskConfig.Set("taskId", 0)
		taskConfig.Set("currentTable", tableName)
		taskConfigs = append(taskConfigs, taskConfig)
	} else {
		step := (minMax.MaxVal - minMax.MinVal) / int64(adviceNumber)
		if step <= 0 {
			step = 1
		}

		for i := 0; i < adviceNumber; i++ {
			taskConfig := job.config.Clone()
			taskConfig.Set("taskId", i)
			taskConfig.Set("currentTable", tableName)

			if i == 0 {
				taskConfig.Set("splitPkRange", map[string]interface{}{
					"min": minMax.MinVal,
					"max": minMax.MinVal + step,
				})
			} else if i == adviceNumber-1 {
				taskConfig.Set("splitPkRange", map[string]interface{}{
					"min": minMax.MinVal + int64(i)*step + 1,
					"max": minMax.MaxVal,
				})
			} else {
				taskConfig.Set("splitPkRange", map[string]interface{}{
					"min": minMax.MinVal + int64(i)*step + 1,
					"max": minMax.MinVal + int64(i+1)*step,
				})
			}

			taskConfigs = append(taskConfigs, taskConfig)
		}
	}

	return taskConfigs, nil
}

func (job *DorisReaderJob) Post() error {
	return nil
}

func (job *DorisReaderJob) Destroy() error {
	return nil
}

// DorisReaderTask Doris读取任务
type DorisReaderTask struct {
	config    config.Configuration
	readerJob *DorisReaderJob
	db        *gorm.DB
	factory   *factory.DataXFactory
}

func NewDorisReaderTask() *DorisReaderTask {
	return &DorisReaderTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *DorisReaderTask) Init(config config.Configuration) error {
	task.config = config

	// 创建ReaderJob来重用配置逻辑
	task.readerJob = NewDorisReaderJob()
	err := task.readerJob.Init(config)
	if err != nil {
		return err
	}

	// 创建数据库连接
	dsn := task.readerJob.convertJdbcUrl(task.readerJob.jdbcUrls[0])
	task.db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect to Doris: %v", err)
	}

	log.Printf("Doris connection established")
	return nil
}

func (task *DorisReaderTask) Prepare() error {
	return nil
}

func (task *DorisReaderTask) StartRead(recordSender plugin.RecordSender) error {
	tableName := task.config.GetString("currentTable")
	if tableName == "" && len(task.readerJob.tables) > 0 {
		tableName = task.readerJob.tables[0]
	}

	// 获取实际的列名
	columns, err := task.readerJob.getTableColumns(tableName)
	if err != nil {
		return err
	}

	// 构建查询SQL
	var query string
	if task.readerJob.querySql != "" {
		query = task.readerJob.querySql
	} else {
		query = task.buildQuery(tableName, columns)
	}

	log.Printf("Executing query: %s", query)

	// 执行查询
	rows, err := task.db.Raw(query).Rows()
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	// 获取列信息
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("failed to get column types: %v", err)
	}

	recordCount := int64(0)
	for rows.Next() {
		// 创建扫描目标
		values := make([]interface{}, len(columnTypes))
		valuePtrs := make([]interface{}, len(columnTypes))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 扫描行数据
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		// 转换为DataX记录
		record := task.factory.GetRecordFactory().CreateRecord()
		for i, value := range values {
			column := task.convertValue(value, columnTypes[i].DatabaseTypeName())
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

	log.Printf("Completed reading %d records", recordCount)
	return nil
}

func (task *DorisReaderTask) buildQuery(tableName string, columns []string) string {
	query := "SELECT " + strings.Join(columns, ", ") + " FROM " + tableName

	// 添加WHERE条件
	conditions := make([]string, 0)

	if task.readerJob.where != "" {
		conditions = append(conditions, task.readerJob.where)
	}

	// 添加splitPk范围条件
	if splitPkRange := task.config.GetMap("splitPkRange"); splitPkRange != nil {
		if min, exists := splitPkRange["min"]; exists {
			if max, exists := splitPkRange["max"]; exists {
				condition := fmt.Sprintf("%s >= %v AND %s <= %v",
					task.readerJob.splitPk, min, task.readerJob.splitPk, max)
				conditions = append(conditions, condition)
			}
		}
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	return query
}

func (task *DorisReaderTask) convertValue(value interface{}, typeName string) element.Column {
	if value == nil {
		return task.factory.GetColumnFactory().CreateStringColumn("")
	}

	switch typeName {
	case "BIGINT", "INT", "TINYINT", "SMALLINT", "LARGINT":
		if intVal, err := strconv.ParseInt(fmt.Sprintf("%v", value), 10, 64); err == nil {
			return task.factory.GetColumnFactory().CreateLongColumn(intVal)
		}
		return task.factory.GetColumnFactory().CreateLongColumn(0)

	case "FLOAT", "DOUBLE", "DECIMAL":
		if floatVal, err := strconv.ParseFloat(fmt.Sprintf("%v", value), 64); err == nil {
			return task.factory.GetColumnFactory().CreateDoubleColumn(floatVal)
		}
		return task.factory.GetColumnFactory().CreateDoubleColumn(0.0)

	case "BOOLEAN":
		if boolVal, err := strconv.ParseBool(fmt.Sprintf("%v", value)); err == nil {
			return task.factory.GetColumnFactory().CreateBoolColumn(boolVal)
		}
		return task.factory.GetColumnFactory().CreateBoolColumn(false)

	case "DATE", "DATETIME", "TIMESTAMP":
		return task.factory.GetColumnFactory().CreateStringColumn(fmt.Sprintf("%v", value))

	default: // VARCHAR, CHAR, TEXT, STRING, MAP, JSON, ARRAY, STRUCT
		return task.factory.GetColumnFactory().CreateStringColumn(fmt.Sprintf("%v", value))
	}
}

func (task *DorisReaderTask) Post() error {
	return nil
}

func (task *DorisReaderTask) Destroy() error {
	if task.db != nil {
		sqlDB, err := task.db.DB()
		if err == nil {
			sqlDB.Close()
		}
	}
	return nil
}