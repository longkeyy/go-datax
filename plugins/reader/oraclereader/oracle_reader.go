package oraclereader

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	// "github.com/godror/godror"
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"

	// _ "github.com/godror/godror"
)

type OracleReaderJob struct {
	*plugin.BaseJob
	conf *config.Configuration
}

type OracleReaderTask struct {
	*plugin.BaseTask
	conf *config.Configuration
	db   *sql.DB
}

func NewOracleReaderJob() *OracleReaderJob {
	job := &OracleReaderJob{
		BaseJob: plugin.NewBaseJob(),
	}
	return job
}

func NewOracleReaderTask() *OracleReaderTask {
	task := &OracleReaderTask{
		BaseTask: plugin.NewBaseTask(),
	}
	return task
}

func (job *OracleReaderJob) Init(ctx context.Context) error {
	job.conf = job.GetPluginJobConf()
	return job.validateConfig()
}

func (job *OracleReaderJob) validateConfig() error {
	connections := job.conf.GetSliceConfiguration("connection")
	if len(connections) == 0 {
		return fmt.Errorf("connection配置不能为空")
	}

	for _, connConf := range connections {
		jdbcUrls := connConf.GetStringSlice("jdbcUrl")
		tables := connConf.GetStringSlice("table")

		if len(jdbcUrls) == 0 {
			return fmt.Errorf("jdbcUrl配置不能为空")
		}
		if len(tables) == 0 {
			return fmt.Errorf("table配置不能为空")
		}
	}

	username := job.conf.GetString("username")
	password := job.conf.GetString("password")
	if username == "" || password == "" {
		return fmt.Errorf("username和password配置不能为空")
	}

	columns := job.conf.GetStringSlice("column")
	if len(columns) == 0 {
		return fmt.Errorf("column配置不能为空")
	}

	return nil
}

func (job *OracleReaderJob) Destroy(ctx context.Context) error {
	return nil
}

func (job *OracleReaderJob) Split(ctx context.Context, adviceNumber int) ([]*config.Configuration, error) {
	var taskConfigs []*config.Configuration

	connections := job.conf.GetSliceConfiguration("connection")

	for _, connConf := range connections {
		jdbcUrls := connConf.GetStringSlice("jdbcUrl")
		tables := connConf.GetStringSlice("table")

		splitPk := job.conf.GetString("splitPk")

		for _, table := range tables {
			for _, jdbcUrl := range jdbcUrls {
				if splitPk != "" && adviceNumber > 1 {
					splits, err := job.splitByPk(jdbcUrl, table, splitPk, adviceNumber)
					if err != nil {
						return nil, fmt.Errorf("split by primary key failed: %v", err)
					}

					for _, splitCondition := range splits {
						taskConfig := job.conf.Clone()
						newConnection := map[string]interface{}{
							"jdbcUrl": []string{jdbcUrl},
							"table":   []string{table},
						}
						taskConfig.Set("connection", []interface{}{newConnection})

						whereCondition := job.conf.GetString("where")
						if whereCondition != "" {
							whereCondition = fmt.Sprintf("(%s) AND (%s)", whereCondition, splitCondition)
						} else {
							whereCondition = splitCondition
						}
						taskConfig.Set("where", whereCondition)

						taskConfigs = append(taskConfigs, taskConfig)
					}
				} else {
					taskConfig := job.conf.Clone()
					newConnection := map[string]interface{}{
						"jdbcUrl": []string{jdbcUrl},
						"table":   []string{table},
					}
					taskConfig.Set("connection", []interface{}{newConnection})
					taskConfigs = append(taskConfigs, taskConfig)
				}
			}
		}
	}

	return taskConfigs, nil
}

func (job *OracleReaderJob) splitByPk(jdbcUrl, table, splitPk string, adviceNumber int) ([]string, error) {
	db, err := job.openConnection(jdbcUrl)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	minMaxQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", splitPk, splitPk, table)
	row := db.QueryRow(minMaxQuery)

	var minVal, maxVal interface{}
	if err := row.Scan(&minVal, &maxVal); err != nil {
		return nil, err
	}

	if minVal == nil || maxVal == nil {
		return []string{}, nil
	}

	var conditions []string

	switch v := minVal.(type) {
	case int64:
		minInt := v
		maxInt := maxVal.(int64)
		step := (maxInt - minInt) / int64(adviceNumber)
		if step == 0 {
			step = 1
		}

		for i := 0; i < adviceNumber; i++ {
			start := minInt + int64(i)*step
			var end int64
			if i == adviceNumber-1 {
				end = maxInt
			} else {
				end = start + step - 1
			}

			condition := fmt.Sprintf("%s >= %d AND %s <= %d", splitPk, start, splitPk, end)
			conditions = append(conditions, condition)
		}
	case string:
		conditions = append(conditions, fmt.Sprintf("%s >= '%s' AND %s <= '%s'", splitPk, v, splitPk, maxVal.(string)))
	default:
		return []string{""}, nil
	}

	return conditions, nil
}

func (job *OracleReaderJob) openConnection(jdbcUrl string) (*sql.DB, error) {
	dsn, err := job.parseConnectionString(jdbcUrl)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("godror", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func (job *OracleReaderJob) parseConnectionString(jdbcUrl string) (string, error) {
	if !strings.HasPrefix(jdbcUrl, "oracle://") {
		return "", fmt.Errorf("invalid Oracle JDBC URL format: %s", jdbcUrl)
	}

	url := strings.TrimPrefix(jdbcUrl, "oracle://")

	username := job.conf.GetString("username")
	password := job.conf.GetString("password")

	return fmt.Sprintf("%s/%s@%s", username, password, url), nil
}

func (task *OracleReaderTask) Init(ctx context.Context) error {
	task.conf = task.GetPluginJobConf()

	connections := task.conf.GetSliceConfiguration("connection")
	if len(connections) == 0 {
		return fmt.Errorf("connection配置不能为空")
	}

	connConf := connections[0]
	jdbcUrls := connConf.GetStringSlice("jdbcUrl")
	if len(jdbcUrls) == 0 {
		return fmt.Errorf("jdbcUrl配置不能为空")
	}

	dsn, err := task.parseConnectionString(jdbcUrls[0])
	if err != nil {
		return err
	}

	db, err := sql.Open("godror", dsn)
	if err != nil {
		return err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return err
	}

	task.db = db

	return task.executeSessionSql()
}

func (task *OracleReaderTask) parseConnectionString(jdbcUrl string) (string, error) {
	if !strings.HasPrefix(jdbcUrl, "oracle://") {
		return "", fmt.Errorf("invalid Oracle JDBC URL format: %s", jdbcUrl)
	}

	url := strings.TrimPrefix(jdbcUrl, "oracle://")

	username := task.conf.GetString("username")
	password := task.conf.GetString("password")

	return fmt.Sprintf("%s/%s@%s", username, password, url), nil
}

func (task *OracleReaderTask) executeSessionSql() error {
	sessionSqls := task.conf.GetStringSlice("session")
	for _, sessionSql := range sessionSqls {
		if _, err := task.db.Exec(sessionSql); err != nil {
			return fmt.Errorf("execute session SQL failed: %v", err)
		}
	}
	return nil
}

func (task *OracleReaderTask) Destroy(ctx context.Context) error {
	if task.db != nil {
		return task.db.Close()
	}
	return nil
}

func (task *OracleReaderTask) StartRead(ctx context.Context, recordSender plugin.RecordSender) error {
	querySql := task.conf.GetString("querySql")

	if querySql == "" {
		columns := task.conf.GetStringSlice("column")
		connections := task.conf.GetSliceConfiguration("connection")
		connConf := connections[0]
		tables := connConf.GetStringSlice("table")
		table := tables[0]

		if len(columns) == 1 && columns[0] == "*" {
			var err error
			columns, err = task.getTableColumns(table)
			if err != nil {
				return fmt.Errorf("get table columns failed: %v", err)
			}
		}

		columnStr := strings.Join(columns, ", ")
		querySql = fmt.Sprintf("SELECT %s FROM %s", columnStr, table)

		whereCondition := task.conf.GetString("where")
		if whereCondition != "" {
			querySql += " WHERE " + whereCondition
		}

		hint := task.conf.GetString("hint")
		if hint != "" {
			querySql = fmt.Sprintf("SELECT /*+ %s */ %s FROM %s", hint, columnStr, table)
			if whereCondition != "" {
				querySql += " WHERE " + whereCondition
			}
		}
	}

	fetchSize := task.conf.GetIntWithDefault("fetchSize", 1024)

	rows, err := task.db.QueryContext(ctx, querySql)
	if err != nil {
		return fmt.Errorf("query failed: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("get columns failed: %v", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("get column types failed: %v", err)
	}

	count := 0
	for rows.Next() {
		record := task.CreateRecord()

		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("scan row failed: %v", err)
		}

		for i, value := range values {
			col, err := task.convertValue(value, columnTypes[i])
			if err != nil {
				return fmt.Errorf("convert value failed: %v", err)
			}
			record.Add(col)
		}

		recordSender.SendWriter(record)
		count++

		if count%fetchSize == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	}

	return rows.Err()
}

func (task *OracleReaderTask) getTableColumns(table string) ([]string, error) {
	query := `
		SELECT column_name
		FROM user_tab_columns
		WHERE table_name = UPPER(?)
		ORDER BY column_id`

	rows, err := task.db.Query(query, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		columns = append(columns, columnName)
	}

	return columns, rows.Err()
}

func (task *OracleReaderTask) convertValue(value interface{}, columnType *sql.ColumnType) (element.Column, error) {
	if value == nil {
		return element.NewDefaultColumn(nil, element.Null), nil
	}

	typeName := strings.ToUpper(columnType.DatabaseTypeName())

	switch typeName {
	case "NUMBER", "INTEGER", "INT", "SMALLINT":
		switch v := value.(type) {
		case int64:
			return element.NewLongColumn(v), nil
		case float64:
			if v == float64(int64(v)) {
				return element.NewLongColumn(int64(v)), nil
			}
			return element.NewDoubleColumn(v), nil
		case string:
			if intVal, err := strconv.ParseInt(v, 10, 64); err == nil {
				return element.NewLongColumn(intVal), nil
			}
			if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
				return element.NewDoubleColumn(floatVal), nil
			}
			return element.NewStringColumn(v), nil
		default:
			return element.NewStringColumn(fmt.Sprintf("%v", v)), nil
		}

	case "NUMERIC", "DECIMAL", "FLOAT", "DOUBLE PRECISION", "REAL":
		switch v := value.(type) {
		case float64:
			return element.NewDoubleColumn(v), nil
		case int64:
			return element.NewDoubleColumn(float64(v)), nil
		case string:
			if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
				return element.NewDoubleColumn(floatVal), nil
			}
			return element.NewStringColumn(v), nil
		default:
			return element.NewStringColumn(fmt.Sprintf("%v", v)), nil
		}

	case "LONG", "CHAR", "NCHAR", "VARCHAR", "VARCHAR2", "NVARCHAR2", "CLOB", "NCLOB":
		return element.NewStringColumn(fmt.Sprintf("%v", value)), nil

	case "DATE", "TIMESTAMP":
		switch v := value.(type) {
		case time.Time:
			return element.NewDateColumn(v), nil
		case string:
			if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
				return element.NewDateColumn(t), nil
			}
			return element.NewStringColumn(v), nil
		default:
			return element.NewStringColumn(fmt.Sprintf("%v", v)), nil
		}

	case "BIT", "BOOL":
		switch v := value.(type) {
		case bool:
			return element.NewBoolColumn(v), nil
		case int64:
			return element.NewBoolColumn(v != 0), nil
		case string:
			if boolVal, err := strconv.ParseBool(v); err == nil {
				return element.NewBoolColumn(boolVal), nil
			}
			return element.NewStringColumn(v), nil
		default:
			return element.NewBoolColumn(fmt.Sprintf("%v", v) != "0"), nil
		}

	case "BLOB", "BFILE", "RAW", "LONG RAW":
		switch v := value.(type) {
		case []byte:
			return element.NewBytesColumn(v), nil
		default:
			return element.NewStringColumn(fmt.Sprintf("%v", value)), nil
		}

	default:
		return element.NewStringColumn(fmt.Sprintf("%v", value)), nil
	}
}