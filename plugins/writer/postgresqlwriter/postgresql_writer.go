package postgresqlwriter

import (
	"github.com/longkeyy/go-datax/common/config"
	"github.com/longkeyy/go-datax/common/plugin"
	"github.com/longkeyy/go-datax/common/database/rdbms/writer"
	"github.com/longkeyy/go-datax/common/factory"
)

// PostgreSQLWriterJob PostgreSQL写入作业，薄包装层委托给通用RDBMS实现
type PostgreSQLWriterJob struct {
	commonRdbmsWriterJob *writer.CommonRdbmsWriterJob
	factory              *factory.DataXFactory
}

func NewPostgreSQLWriterJob() *PostgreSQLWriterJob {
	return &PostgreSQLWriterJob{
		commonRdbmsWriterJob: writer.NewCommonRdbmsWriterJob(writer.PostgreSQL),
		factory:              factory.GetGlobalFactory(),
	}
}

func (job *PostgreSQLWriterJob) Init(config config.Configuration) error {
	return job.commonRdbmsWriterJob.Init(config)
}

func (job *PostgreSQLWriterJob) Split(mandatoryNumber int) ([]config.Configuration, error) {
	return job.commonRdbmsWriterJob.Split(mandatoryNumber)
}

func (job *PostgreSQLWriterJob) Prepare() error {
	return job.commonRdbmsWriterJob.Prepare()
}

func (job *PostgreSQLWriterJob) Post() error {
	return job.commonRdbmsWriterJob.Post()
}

func (job *PostgreSQLWriterJob) Destroy() error {
	return job.commonRdbmsWriterJob.Destroy()
}

// PostgreSQLWriterTask PostgreSQL写入任务，薄包装层委托给通用RDBMS实现
type PostgreSQLWriterTask struct {
	commonRdbmsWriterTask *writer.CommonRdbmsWriterTask
	factory               *factory.DataXFactory
}

func NewPostgreSQLWriterTask() *PostgreSQLWriterTask {
	return &PostgreSQLWriterTask{
		commonRdbmsWriterTask: writer.NewCommonRdbmsWriterTask(writer.PostgreSQL),
		factory:               factory.GetGlobalFactory(),
	}
}

func (task *PostgreSQLWriterTask) Init(config config.Configuration) error {
	return task.commonRdbmsWriterTask.Init(config)
}

func (task *PostgreSQLWriterTask) Prepare() error {
	return task.commonRdbmsWriterTask.Prepare()
}

func (task *PostgreSQLWriterTask) StartWrite(recordReceiver plugin.RecordReceiver) error {
	return task.commonRdbmsWriterTask.StartWrite(recordReceiver)
}

func (task *PostgreSQLWriterTask) Post() error {
	return task.commonRdbmsWriterTask.Post()
}

func (task *PostgreSQLWriterTask) Destroy() error {
	return task.commonRdbmsWriterTask.Destroy()
}