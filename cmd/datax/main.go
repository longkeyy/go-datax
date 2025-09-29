package main

import (
	"github.com/longkeyy/go-datax/core/engine"

	// Import plugins to trigger automatic registration via init() functions
	_ "github.com/longkeyy/go-datax/plugins/reader/cassandrareader"
	_ "github.com/longkeyy/go-datax/plugins/reader/clickhousereader"
	_ "github.com/longkeyy/go-datax/plugins/reader/dorisreader"
	_ "github.com/longkeyy/go-datax/plugins/reader/ftpreader"
	_ "github.com/longkeyy/go-datax/plugins/reader/gaussdbreader"
	_ "github.com/longkeyy/go-datax/plugins/reader/hdfsreader"
	_ "github.com/longkeyy/go-datax/plugins/reader/jsonfilereader"
	_ "github.com/longkeyy/go-datax/plugins/reader/mongoreader"
	_ "github.com/longkeyy/go-datax/plugins/reader/mysqlreader"
	_ "github.com/longkeyy/go-datax/plugins/reader/oceanbasereader"
	_ "github.com/longkeyy/go-datax/plugins/reader/oraclereader"
	_ "github.com/longkeyy/go-datax/plugins/reader/postgresqlreader"
	_ "github.com/longkeyy/go-datax/plugins/reader/sqlitereader"
	_ "github.com/longkeyy/go-datax/plugins/reader/sqlserverreader"
	_ "github.com/longkeyy/go-datax/plugins/reader/starrocksreader"
	_ "github.com/longkeyy/go-datax/plugins/reader/sybasereader"
	_ "github.com/longkeyy/go-datax/plugins/reader/streamreader"
	_ "github.com/longkeyy/go-datax/plugins/reader/tdenginereader"
	_ "github.com/longkeyy/go-datax/plugins/reader/txtfilereader"
	_ "github.com/longkeyy/go-datax/plugins/writer/cassandrawriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/clickhousewriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/databendwriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/doriswriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/elasticsearchwriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/ftpwriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/gaussdbwriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/hdfswriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/jsonfilewriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/mongowriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/mysqlwriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/neo4jwriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/oceanbasewriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/oraclewriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/postgresqlwriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/sqlitewriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/sqlserverwriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/starrockswriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/sybasewriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/streamwriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/tdenginewriter"
	_ "github.com/longkeyy/go-datax/plugins/writer/txtfilewriter"
)

var Version = "v1.0.0"

func main() {
	// Plugins automatically register themselves via init() functions during import
	engine.Main(Version)
}
