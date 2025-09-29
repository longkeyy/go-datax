package sybasereader

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

	// 使用thda/tds纯Go Sybase ASE/TDS驱动
	_ "github.com/thda/tds"
)

const (
	DefaultFetchSize = 1024
)

// SybaseReaderJob Sybase读取作业
type SybaseReaderJob struct {
	config   config.Configuration
	username string
	password string
	jdbcUrls []string
	tables   []string
	columns  []string
	where    string
	splitPk  string
	querySql string // 支持自定义查询SQL
	factory  *factory.DataXFactory
}

func NewSybaseReaderJob() *SybaseReaderJob {
	return &SybaseReaderJob{
		factory: factory.GetGlobalFactory(),
	}
}

func (job *SybaseReaderJob) Init(config config.Configuration) error {
	job.config = config

	// 获取数据库连接参数
	job.username = config.GetString("parameter.username")
	job.password = config.GetString("parameter.password")

	// 获取连接配置列表
	connections := config.GetListConfiguration("parameter.connection")
	for _, conn := range connections {
		// 获取JDBC URL列表
		for _, url := range conn.GetStringList("jdbcUrl") {
			job.jdbcUrls = append(job.jdbcUrls, url)
		}
		// 获取表列表
		for _, table := range conn.GetStringList("table") {
			job.tables = append(job.tables, table)
		}
	}

	// 获取查询列
	if columns := config.GetStringList("parameter.column"); len(columns) > 0 {
		job.columns = columns
	} else {
		job.columns = []string{"*"}
	}

	// 获取WHERE条件
	job.where = config.GetString("parameter.where")

	// 获取分片主键
	job.splitPk = config.GetString("parameter.splitPk")

	// 获取自定义查询SQL
	job.querySql = config.GetString("parameter.querySql")

	// 处理fetchSize，按照Java版本的逻辑
	fetchSize := config.GetIntWithDefault("parameter.fetchSize", DefaultFetchSize)
	if fetchSize < 1 {
		logger.Component().WithComponent("sybasereader").Warn("对 sybasereader 需要配置 fetchSize, 对性能提升有较大影响 请配置fetchSize.")
	}
	job.config.Set("parameter.fetchSize", fetchSize)

	return nil
}

func (job *SybaseReaderJob) PreCheck() error {
	// 验证必要参数
	if len(job.jdbcUrls) == 0 {
		return fmt.Errorf("sybase reader: jdbcUrl不能为空")
	}
	if job.username == "" {
		return fmt.Errorf("sybase reader: username不能为空")
	}
	if job.password == "" {
		return fmt.Errorf("sybase reader: password不能为空")
	}

	// 测试连接
	for _, jdbcUrl := range job.jdbcUrls {
		dsn := job.buildDSN(jdbcUrl)
		db, err := sql.Open("tds", dsn)
		if err != nil {
			return fmt.Errorf("sybase reader: 连接数据库失败: %v", err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			return fmt.Errorf("sybase reader: 数据库连接测试失败: %v", err)
		}
	}

	return nil
}

func (job *SybaseReaderJob) Split(adviceNumber int) ([]config.Configuration, error) {
	var configs []config.Configuration

	if job.querySql != "" {
		// 使用自定义SQL，不能分片
		newConfig := job.config.Clone()
		configs = append(configs, newConfig)
		return configs, nil
	}

	// 根据表和JDBC URL进行分片
	for _, jdbcUrl := range job.jdbcUrls {
		for _, table := range job.tables {
			newConfig := job.config.Clone()

			// 设置单个任务的配置
			newConfig.Set("parameter.connection", []map[string]interface{}{
				{
					"jdbcUrl": []string{jdbcUrl},
					"table":   []string{table},
				},
			})

			configs = append(configs, newConfig)
		}
	}

	return configs, nil
}

func (job *SybaseReaderJob) Post() error {
	return nil
}

func (job *SybaseReaderJob) Destroy() error {
	return nil
}

// 构建Sybase ASE连接字符串
func (job *SybaseReaderJob) buildDSN(jdbcUrl string) string {
	// 将JDBC URL转换为thda/tds驱动支持的格式
	// jdbc:sybase:Tds:host:port/database -> tds://username:password@host:port/database

	// 解析JDBC URL
	// 格式: jdbc:sybase:Tds:host:port/database 或 jdbc:sybase:Tds:host:port?database=dbname
	if strings.HasPrefix(jdbcUrl, "jdbc:sybase:Tds:") {
		url := strings.TrimPrefix(jdbcUrl, "jdbc:sybase:Tds:")

		// 检查是否包含数据库名
		var host, database string
		if strings.Contains(url, "/") {
			parts := strings.SplitN(url, "/", 2)
			host = parts[0]
			database = parts[1]
			// 去掉可能的查询参数
			if strings.Contains(database, "?") {
				database = strings.Split(database, "?")[0]
			}
		} else if strings.Contains(url, "?") {
			parts := strings.SplitN(url, "?", 2)
			host = parts[0]
			// 解析查询参数中的database
			params := strings.Split(parts[1], "&")
			for _, param := range params {
				if strings.HasPrefix(param, "database=") {
					database = strings.TrimPrefix(param, "database=")
					break
				}
			}
		} else {
			host = url
		}

		dsn := fmt.Sprintf("tds://%s:%s@%s", job.username, job.password, host)
		if database != "" {
			dsn += "/" + database
		}

		return dsn
	}

	// 如果不是标准JDBC URL，尝试直接使用
	return jdbcUrl
}

// SybaseReaderTask Sybase读取任务
type SybaseReaderTask struct {
	config    config.Configuration
	db        *sql.DB
	factory   *factory.DataXFactory
	taskGroup int
	taskId    int
}

func NewSybaseReaderTask() *SybaseReaderTask {
	return &SybaseReaderTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *SybaseReaderTask) Init(config config.Configuration) error {
	task.config = config
	return nil
}

func (task *SybaseReaderTask) SetTaskInfo(taskGroupId int, taskId int) {
	task.taskGroup = taskGroupId
	task.taskId = taskId
}

func (task *SybaseReaderTask) StartRead(recordSender plugin.RecordSender) error {
	username := task.config.GetString("parameter.username")
	password := task.config.GetString("parameter.password")

	// 获取连接信息
	connections := task.config.GetListConfiguration("parameter.connection")
	if len(connections) == 0 {
		return fmt.Errorf("sybase reader task: 没有找到数据库连接配置")
	}

	conn := connections[0]
	jdbcUrls := conn.GetStringList("jdbcUrl")
	tables := conn.GetStringList("table")

	if len(jdbcUrls) == 0 || len(tables) == 0 {
		return fmt.Errorf("sybase reader task: jdbcUrl或table配置为空")
	}

	jdbcUrl := jdbcUrls[0]
	tableName := tables[0]

	// 构建DSN并连接数据库
	job := &SybaseReaderJob{username: username, password: password}
	dsn := job.buildDSN(jdbcUrl)

	db, err := sql.Open("tds", dsn)
	if err != nil {
		return fmt.Errorf("sybase reader task: 连接数据库失败: %v", err)
	}
	defer db.Close()
	task.db = db

	// 构建查询SQL
	querySql := task.config.GetString("parameter.querySql")
	if querySql == "" {
		columns := task.config.GetStringList("parameter.column")
		columnStr := "*"
		if len(columns) > 0 {
			columnStr = strings.Join(columns, ", ")
		}

		querySql = fmt.Sprintf("SELECT %s FROM %s", columnStr, tableName)

		where := task.config.GetString("parameter.where")
		if where != "" {
			querySql += " WHERE " + where
		}
	}

	logger.Component().WithComponent("sybasereader").Info("开始执行查询",
		zap.String("sql", querySql))

	// 执行查询
	rows, err := db.Query(querySql)
	if err != nil {
		return fmt.Errorf("sybase reader task: 查询执行失败: %v", err)
	}
	defer rows.Close()

	// 获取列信息
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("sybase reader task: 获取列信息失败: %v", err)
	}

	// 读取数据并发送记录
	values := make([]interface{}, len(columnTypes))
	valuePtrs := make([]interface{}, len(columnTypes))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	recordCount := 0
	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return fmt.Errorf("sybase reader task: 扫描行数据失败: %v", err)
		}

		// 创建记录
		record := element.NewRecord()
		for i, value := range values {
			column := task.convertToColumn(value, columnTypes[i])
			record.AddColumn(column)
		}

		// 发送记录
		recordSender.SendRecord(record)
		recordCount++
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("sybase reader task: 行迭代错误: %v", err)
	}

	logger.Component().WithComponent("sybasereader").Info("读取完成",
		zap.Int("recordCount", recordCount))

	return nil
}

func (task *SybaseReaderTask) Post() error {
	return nil
}

func (task *SybaseReaderTask) Destroy() error {
	if task.db != nil {
		task.db.Close()
	}
	return nil
}

// 将数据库值转换为DataX Column
func (task *SybaseReaderTask) convertToColumn(value interface{}, columnType *sql.ColumnType) element.Column {
	columnFactory := task.factory.GetColumnFactory()
	if value == nil {
		return columnFactory.CreateStringColumn("")
	}

	switch v := value.(type) {
	case []byte:
		// 处理字节数组，通常是字符串类型
		return columnFactory.CreateStringColumn(string(v))
	case string:
		return columnFactory.CreateStringColumn(v)
	case int:
		return columnFactory.CreateLongColumn(int64(v))
	case int8:
		return columnFactory.CreateLongColumn(int64(v))
	case int16:
		return columnFactory.CreateLongColumn(int64(v))
	case int32:
		return columnFactory.CreateLongColumn(int64(v))
	case int64:
		return columnFactory.CreateLongColumn(v)
	case float32:
		return columnFactory.CreateDoubleColumn(float64(v))
	case float64:
		return columnFactory.CreateDoubleColumn(v)
	case bool:
		return columnFactory.CreateBoolColumn(v)
	case time.Time:
		return columnFactory.CreateDateColumn(v)
	default:
		// 对于其他类型，转换为字符串
		return columnFactory.CreateStringColumn(fmt.Sprintf("%v", v))
	}
}