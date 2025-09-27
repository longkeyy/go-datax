package oraclewriter

import (
	"github.com/longkeyy/go-datax/common/plugin"
)

func init() {
	plugin.RegisterWriterJob("oraclewriter", func() plugin.WriterJob {
		return NewOracleWriterJob()
	})

	plugin.RegisterWriterTask("oraclewriter", func() plugin.WriterTask {
		return NewOracleWriterTask()
	})
}