package sybasewriter

import (
	"database/sql"
	"fmt"
	"strings"

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
	DefaultBatchSize = 1024
	WriteMode_Insert = "insert"
	WriteMode_Replace = "replace"
	WriteMode_Update = "update"
)

// SybaseWriterJob Sybase写入作业
type SybaseWriterJob struct {
	config    config.Configuration
	username  string
	password  string
	jdbcUrls  []string
	tables    []string
	columns   []string
	writeMode string
	batchSize int
	factory   *factory.DataXFactory
}

func NewSybaseWriterJob() *SybaseWriterJob {
	return &SybaseWriterJob{
		factory: factory.GetGlobalFactory(),
	}
}

func (job *SybaseWriterJob) Init(config config.Configuration) error {
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

	// 获取写入列
	if columns := config.GetStringList("parameter.column"); len(columns) > 0 {
		job.columns = columns
	}

	// 获取写入模式
	job.writeMode = config.GetStringWithDefault("parameter.writeMode", WriteMode_Insert)

	// 获取批处理大小
	job.batchSize = config.GetIntWithDefault("parameter.batchSize", DefaultBatchSize)

	return nil
}

func (job *SybaseWriterJob) PreCheck() error {
	// 验证必要参数
	if len(job.jdbcUrls) == 0 {
		return fmt.Errorf("sybase writer: jdbcUrl不能为空")
	}
	if len(job.tables) == 0 {
		return fmt.Errorf("sybase writer: table不能为空")
	}
	if job.username == "" {
		return fmt.Errorf("sybase writer: username不能为空")
	}
	if job.password == "" {
		return fmt.Errorf("sybase writer: password不能为空")
	}

	// 验证写入模式
	switch job.writeMode {
	case WriteMode_Insert, WriteMode_Replace, WriteMode_Update:
		// 支持的模式
	default:
		return fmt.Errorf("sybase writer: 不支持的写入模式: %s", job.writeMode)
	}

	// 测试连接
	for _, jdbcUrl := range job.jdbcUrls {
		dsn := job.buildDSN(jdbcUrl)
		db, err := sql.Open("tds", dsn)
		if err != nil {
			return fmt.Errorf("sybase writer: 连接数据库失败: %v", err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			return fmt.Errorf("sybase writer: 数据库连接测试失败: %v", err)
		}
	}

	return nil
}

func (job *SybaseWriterJob) Prepare() error {
	// 对于Sybase，我们通常不需要特殊的准备步骤
	// 权限检验等在Java版本中也是注释掉的
	return nil
}

func (job *SybaseWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	var configs []config.Configuration

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

func (job *SybaseWriterJob) Post() error {
	return nil
}

func (job *SybaseWriterJob) Destroy() error {
	return nil
}

// 支持失败恢复
func (job *SybaseWriterJob) SupportFailOver() bool {
	return job.writeMode == WriteMode_Replace
}

// 构建Sybase ASE连接字符串
func (job *SybaseWriterJob) buildDSN(jdbcUrl string) string {
	// 将JDBC URL转换为thda/tds驱动支持的格式
	// jdbc:sybase:Tds:host:port/database -> tds://username:password@host:port/database
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

// SybaseWriterTask Sybase写入任务
type SybaseWriterTask struct {
	config      config.Configuration
	db          *sql.DB
	factory     *factory.DataXFactory
	taskGroup   int
	taskId      int
	writeMode   string
	batchSize   int
	columns     []string
	tableName   string
	insertStmt  *sql.Stmt
	replaceStmt *sql.Stmt
	updateStmt  *sql.Stmt
}

func NewSybaseWriterTask() *SybaseWriterTask {
	return &SybaseWriterTask{
		factory: factory.GetGlobalFactory(),
	}
}

func (task *SybaseWriterTask) Init(config config.Configuration) error {
	task.config = config
	task.writeMode = config.GetStringWithDefault("parameter.writeMode", WriteMode_Insert)
	task.batchSize = config.GetIntWithDefault("parameter.batchSize", DefaultBatchSize)

	// 获取列配置
	if columns := config.GetStringList("parameter.column"); len(columns) > 0 {
		task.columns = columns
	}

	return nil
}

func (task *SybaseWriterTask) SetTaskInfo(taskGroupId int, taskId int) {
	task.taskGroup = taskGroupId
	task.taskId = taskId
}

func (task *SybaseWriterTask) Prepare() error {
	username := task.config.GetString("parameter.username")
	password := task.config.GetString("parameter.password")

	// 获取连接信息
	connections := task.config.GetListConfiguration("parameter.connection")
	if len(connections) == 0 {
		return fmt.Errorf("sybase writer task: 没有找到数据库连接配置")
	}

	conn := connections[0]
	jdbcUrls := conn.GetStringList("jdbcUrl")
	tables := conn.GetStringList("table")

	if len(jdbcUrls) == 0 || len(tables) == 0 {
		return fmt.Errorf("sybase writer task: jdbcUrl或table配置为空")
	}

	jdbcUrl := jdbcUrls[0]
	task.tableName = tables[0]

	// 构建DSN并连接数据库
	job := &SybaseWriterJob{username: username, password: password}
	dsn := job.buildDSN(jdbcUrl)

	db, err := sql.Open("tds", dsn)
	if err != nil {
		return fmt.Errorf("sybase writer task: 连接数据库失败: %v", err)
	}
	task.db = db

	// 准备SQL语句
	if err := task.prepareStatements(); err != nil {
		return err
	}

	return nil
}

func (task *SybaseWriterTask) prepareStatements() error {
	if len(task.columns) == 0 {
		return fmt.Errorf("sybase writer task: columns配置为空")
	}

	columnStr := strings.Join(task.columns, ", ")
	placeholders := make([]string, len(task.columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	placeholderStr := strings.Join(placeholders, ", ")

	// 准备INSERT语句
	insertSql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		task.tableName, columnStr, placeholderStr)

	stmt, err := task.db.Prepare(insertSql)
	if err != nil {
		return fmt.Errorf("sybase writer task: 准备INSERT语句失败: %v", err)
	}
	task.insertStmt = stmt

	// 如果写入模式是REPLACE，准备相应的语句
	if task.writeMode == WriteMode_Replace {
		// Sybase ASE不直接支持REPLACE INTO，我们使用DELETE + INSERT的方式
		// 这里简化处理，实际应该根据主键或唯一约束来处理
		logger.Component().WithComponent("sybasewriter").Warn(
			"Sybase ASE不直接支持REPLACE模式，将使用INSERT模式")
	}

	return nil
}

func (task *SybaseWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	logger.Component().WithComponent("sybasewriter").Info("开始写入数据",
		zap.String("table", task.tableName),
		zap.String("writeMode", task.writeMode))

	// 开始事务
	tx, err := task.db.Begin()
	if err != nil {
		return fmt.Errorf("sybase writer task: 开始事务失败: %v", err)
	}
	defer tx.Rollback()

	recordCount := 0
	batchCount := 0
	var batch []element.Record

	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			// 检查是否是通道关闭错误
			if err.Error() == "channel closed" {
				break
			}
			return fmt.Errorf("sybase writer task: 读取记录失败: %v", err)
		}
		if record == nil {
			break
		}

		batch = append(batch, record)
		recordCount++

		// 当批量达到指定大小时执行写入
		if len(batch) >= task.batchSize {
			if err := task.writeBatch(tx, batch); err != nil {
				return fmt.Errorf("sybase writer task: 批量写入失败: %v", err)
			}
			batchCount++
			batch = batch[:0] // 清空批次
		}
	}

	// 写入剩余的记录
	if len(batch) > 0 {
		if err := task.writeBatch(tx, batch); err != nil {
			return fmt.Errorf("sybase writer task: 写入剩余记录失败: %v", err)
		}
		batchCount++
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sybase writer task: 提交事务失败: %v", err)
	}

	logger.Component().WithComponent("sybasewriter").Info("写入完成",
		zap.Int("recordCount", recordCount),
		zap.Int("batchCount", batchCount))

	return nil
}

func (task *SybaseWriterTask) writeBatch(tx *sql.Tx, batch []element.Record) error {
	stmt := tx.Stmt(task.insertStmt)
	defer stmt.Close()

	for _, record := range batch {
		values := make([]interface{}, len(task.columns))

		for i := 0; i < len(task.columns) && i < record.GetColumnNumber(); i++ {
			column := record.GetColumn(i)
			values[i] = task.convertFromColumn(column)
		}

		_, err := stmt.Exec(values...)
		if err != nil {
			return fmt.Errorf("执行写入失败: %v", err)
		}
	}

	return nil
}

func (task *SybaseWriterTask) convertFromColumn(column element.Column) interface{} {
	if column == nil || column.GetRawData() == nil {
		return nil
	}

	// 使用新的Column接口方法
	switch column.GetType() {
	case element.TypeString:
		return column.GetAsString()
	case element.TypeLong:
		val, _ := column.GetAsLong()
		return val
	case element.TypeDouble:
		val, _ := column.GetAsDouble()
		return val
	case element.TypeDate:
		val, _ := column.GetAsDate()
		return val
	case element.TypeBool:
		val, _ := column.GetAsBool()
		return val
	case element.TypeBytes:
		val, _ := column.GetAsBytes()
		return val
	default:
		// 对于其他类型作为字符串处理
		return column.GetAsString()
	}
}

func (task *SybaseWriterTask) Post() error {
	return nil
}

func (task *SybaseWriterTask) Destroy() error {
	if task.insertStmt != nil {
		task.insertStmt.Close()
	}
	if task.replaceStmt != nil {
		task.replaceStmt.Close()
	}
	if task.updateStmt != nil {
		task.updateStmt.Close()
	}
	if task.db != nil {
		task.db.Close()
	}
	return nil
}

func (task *SybaseWriterTask) SupportFailOver() bool {
	return task.writeMode == WriteMode_Replace
}