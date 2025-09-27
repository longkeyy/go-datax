package oraclewriter

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/element"
	"github.com/longkeyy/go-datax/common/plugin"

	// _ "github.com/godror/godror"
)

type OracleWriterJob struct {
	*plugin.BaseJob
	conf *config.Configuration
}

type OracleWriterTask struct {
	*plugin.BaseTask
	conf    *config.Configuration
	db      *sql.DB
	stmt    *sql.Stmt
	columns []string
}

func NewOracleWriterJob() *OracleWriterJob {
	job := &OracleWriterJob{
		BaseJob: plugin.NewBaseJob(),
	}
	return job
}

func NewOracleWriterTask() *OracleWriterTask {
	task := &OracleWriterTask{
		BaseTask: plugin.NewBaseTask(),
	}
	return task
}

func (job *OracleWriterJob) Init(ctx context.Context) error {
	job.conf = job.GetPluginJobConf()
	return job.validateConfig()
}

func (job *OracleWriterJob) validateConfig() error {
	connections := job.conf.GetSliceConfiguration("connection")
	if len(connections) == 0 {
		return fmt.Errorf("connection配置不能为空")
	}

	for _, connConf := range connections {
		jdbcUrl := connConf.GetString("jdbcUrl")
		tables := connConf.GetStringSlice("table")

		if jdbcUrl == "" {
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

	writeMode := job.conf.GetStringWithDefault("writeMode", "INSERT")
	if writeMode != "INSERT" && writeMode != "MERGE" {
		return fmt.Errorf("writeMode只支持INSERT和MERGE")
	}

	return nil
}

func (job *OracleWriterJob) Destroy(ctx context.Context) error {
	return nil
}

func (job *OracleWriterJob) Split(ctx context.Context, adviceNumber int) ([]*config.Configuration, error) {
	var taskConfigs []*config.Configuration

	connections := job.conf.GetSliceConfiguration("connection")

	for _, connConf := range connections {
		jdbcUrl := connConf.GetString("jdbcUrl")
		tables := connConf.GetStringSlice("table")

		for _, table := range tables {
			taskConfig := job.conf.Clone()
			newConnection := map[string]interface{}{
				"jdbcUrl": jdbcUrl,
				"table":   []string{table},
			}
			taskConfig.Set("connection", []interface{}{newConnection})
			taskConfigs = append(taskConfigs, taskConfig)
		}
	}

	return taskConfigs, nil
}

func (task *OracleWriterTask) Init(ctx context.Context) error {
	task.conf = task.GetPluginJobConf()

	connections := task.conf.GetSliceConfiguration("connection")
	if len(connections) == 0 {
		return fmt.Errorf("connection配置不能为空")
	}

	connConf := connections[0]
	jdbcUrl := connConf.GetString("jdbcUrl")
	if jdbcUrl == "" {
		return fmt.Errorf("jdbcUrl配置不能为空")
	}

	dsn, err := task.parseConnectionString(jdbcUrl)
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

	if err := task.executeSessionSql(); err != nil {
		return err
	}

	if err := task.executePreSql(); err != nil {
		return err
	}

	return task.prepareStatement()
}

func (task *OracleWriterTask) parseConnectionString(jdbcUrl string) (string, error) {
	if !strings.HasPrefix(jdbcUrl, "oracle://") {
		return "", fmt.Errorf("invalid Oracle JDBC URL format: %s", jdbcUrl)
	}

	url := strings.TrimPrefix(jdbcUrl, "oracle://")

	username := task.conf.GetString("username")
	password := task.conf.GetString("password")

	return fmt.Sprintf("%s/%s@%s", username, password, url), nil
}

func (task *OracleWriterTask) executeSessionSql() error {
	sessionSqls := task.conf.GetStringSlice("session")
	for _, sessionSql := range sessionSqls {
		if _, err := task.db.Exec(sessionSql); err != nil {
			return fmt.Errorf("execute session SQL failed: %v", err)
		}
	}
	return nil
}

func (task *OracleWriterTask) executePreSql() error {
	preSqls := task.conf.GetStringSlice("preSql")
	for _, preSql := range preSqls {
		if _, err := task.db.Exec(preSql); err != nil {
			return fmt.Errorf("execute preSql failed: %v", err)
		}
	}
	return nil
}

func (task *OracleWriterTask) executePostSql() error {
	postSqls := task.conf.GetStringSlice("postSql")
	for _, postSql := range postSqls {
		if _, err := task.db.Exec(postSql); err != nil {
			return fmt.Errorf("execute postSql failed: %v", err)
		}
	}
	return nil
}

func (task *OracleWriterTask) prepareStatement() error {
	connections := task.conf.GetSliceConfiguration("connection")
	connConf := connections[0]
	tables := connConf.GetStringSlice("table")
	table := tables[0]

	columns := task.conf.GetStringSlice("column")
	if len(columns) == 1 && columns[0] == "*" {
		var err error
		columns, err = task.getTableColumns(table)
		if err != nil {
			return fmt.Errorf("get table columns failed: %v", err)
		}
	}
	task.columns = columns

	writeMode := task.conf.GetStringWithDefault("writeMode", "INSERT")
	var sqlStatement string

	if writeMode == "INSERT" {
		placeholders := make([]string, len(columns))
		for i := range placeholders {
			placeholders[i] = fmt.Sprintf(":col%d", i+1)
		}

		sqlStatement = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			table,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "))
	} else if writeMode == "MERGE" {
		if len(columns) == 0 {
			return fmt.Errorf("MERGE mode requires column configuration")
		}

		placeholders := make([]string, len(columns))
		updateClauses := make([]string, len(columns)-1)
		for i := range placeholders {
			placeholders[i] = fmt.Sprintf(":col%d", i+1)
			if i > 0 {
				updateClauses[i-1] = fmt.Sprintf("t.%s = s.%s", columns[i], columns[i])
			}
		}

		sourceColumns := strings.Join(columns, ", ")
		sourceValues := strings.Join(placeholders, ", ")
		updateClause := strings.Join(updateClauses, ", ")
		insertColumns := strings.Join(columns, ", ")
		insertValues := strings.Join(placeholders, ", ")

		sqlStatement = fmt.Sprintf(`
			MERGE INTO %s t
			USING (SELECT %s FROM dual) s
			ON (t.%s = s.%s)
			WHEN MATCHED THEN
				UPDATE SET %s
			WHEN NOT MATCHED THEN
				INSERT (%s) VALUES (%s)`,
			table,
			sourceValues,
			columns[0], columns[0],
			updateClause,
			insertColumns,
			insertValues)
	}

	stmt, err := task.db.Prepare(sqlStatement)
	if err != nil {
		return fmt.Errorf("prepare statement failed: %v", err)
	}

	task.stmt = stmt
	return nil
}

func (task *OracleWriterTask) getTableColumns(table string) ([]string, error) {
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

func (task *OracleWriterTask) Destroy(ctx context.Context) error {
	if err := task.executePostSql(); err != nil {
		return err
	}

	if task.stmt != nil {
		task.stmt.Close()
	}

	if task.db != nil {
		return task.db.Close()
	}
	return nil
}

func (task *OracleWriterTask) StartWrite(ctx context.Context, recordReceiver plugin.RecordReceiver) error {
	batchSize := task.conf.GetIntWithDefault("batchSize", 1024)
	batch := make([]plugin.Record, 0, batchSize)

	for {
		record, err := recordReceiver.GetFromReader()
		if err != nil {
			if err == plugin.ErrChannelClosed {
				break
			}
			return err
		}

		batch = append(batch, record)

		if len(batch) >= batchSize {
			if err := task.writeBatch(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	if len(batch) > 0 {
		if err := task.writeBatch(ctx, batch); err != nil {
			return err
		}
	}

	return nil
}

func (task *OracleWriterTask) writeBatch(ctx context.Context, batch []plugin.Record) error {
	tx, err := task.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction failed: %v", err)
	}
	defer tx.Rollback()

	stmt := tx.Stmt(task.stmt)
	defer stmt.Close()

	for _, record := range batch {
		values := make([]interface{}, len(task.columns))

		for i := 0; i < len(task.columns) && i < record.GetColumnNumber(); i++ {
			column := record.GetColumn(i)
			values[i] = task.convertToOracleValue(column)
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("execute statement failed: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction failed: %v", err)
	}

	return nil
}

func (task *OracleWriterTask) convertToOracleValue(column element.Column) interface{} {
	if column.IsNull() {
		return nil
	}

	switch col := column.(type) {
	case *element.StringColumn:
		return col.AsString()
	case *element.LongColumn:
		return col.AsInt64()
	case *element.DoubleColumn:
		return col.AsFloat64()
	case *element.DateColumn:
		return col.AsTime()
	case *element.BoolColumn:
		if col.AsBool() {
			return 1
		}
		return 0
	case *element.BytesColumn:
		return col.AsBytes()
	default:
		return fmt.Sprintf("%v", column.AsString())
	}
}