English | [简体中文](README.md)

# go-datax

Go version of DataX data synchronization tool, 100% compatible with DataX JSON configuration format.

## ✨ Key Features

- 🚀 **High Performance**: Go implementation with high-concurrency data transfer
- 🔧 **Zero Dependencies**: Single binary file, no additional installation required
- 📊 **30+ Data Sources**: Support for mainstream databases and storage systems
- ✅ **100% Compatible**: Full compatibility with DataX Java configuration files
- ⚡ **Go Optimizations**: Enhanced splitPk support and JSON file processing

## 🆕 Go Version Enhancements

### Non-Numeric splitPk Support
Unlike the Java version which only supports numeric splitPk, the Go version supports:
- **Numeric Types**: `int`, `bigint`, `decimal`, etc. (compatible with Java version)
- **String Types**: `varchar`, `text`, and other string field partitioning
- **Date Types**: `date`, `timestamp`, and other time field partitioning

```json
{
  "reader": {
    "parameter": {
      "splitPk": "create_time",  // Support for date-time field partitioning
      "where": "status = 'active'"
    }
  }
}
```

### JSON File Data Source Support
New dedicated JSON file reader and writer plugins:
- **JsonFileReader**: Read JSON/JSONL files
- **JsonFileWriter**: Write JSON format files
- **Auto Format Detection**: Automatically detects standard JSON vs JSONL format

```json
{
  "reader": {
    "name": "jsonfilereader",
    "parameter": {
      "path": ["/data/input.json"],
      "encoding": "UTF-8"
    }
  }
}
```

## 🏗️ Architecture Design

Adopts DataX three-layer architecture design:

```
Engine Layer
├── JobContainer
└── TaskGroupContainer
    ├── Reader Task
    ├── Channel
    └── Writer Task
```

## 📦 Supported Data Sources

DataX has a comprehensive plugin ecosystem with mainstream RDBMS databases, NoSQL, and big data computing systems already integrated.

| Type | Data Source | Reader | Writer | Documentation |
|------|-------------|:------:|:------:|---------------|
| **Relational Databases** | MySQL | ✅ | ✅ | [Reader](docs/plugins/mysqlreader.md) · [Writer](docs/plugins/mysqlwriter.md) |
| | PostgreSQL | ✅ | ✅ | [Reader](docs/plugins/postgresqlreader.md) · [Writer](docs/plugins/postgresqlwriter.md) |
| | Oracle | ✅ | ✅ | [Reader](docs/plugins/oraclereader.md) · [Writer](docs/plugins/oraclewriter.md) |
| | SQL Server | ✅ | ✅ | [Reader](docs/plugins/sqlserverreader.md) · [Writer](docs/plugins/sqlserverwriter.md) |
| | SQLite | ✅ | ✅ | [Reader](docs/plugins/sqlitereader.md) · [Writer](docs/plugins/sqlitewriter.md) |
| | OceanBase | ✅ | ✅ | [Reader](docs/plugins/oceanbasereader.md) · [Writer](docs/plugins/oceanbasewriter.md) |
| | GaussDB | ✅ | ✅ | [Reader](docs/plugins/gaussdbreader.md) · [Writer](docs/plugins/gaussdbwriter.md) |
| | Sybase ASE | ✅ | ✅ | [Reader](docs/plugins/sybasereader.md) · [Writer](docs/plugins/sybasewriter.md) |
| **Big Data Storage** | ClickHouse | ✅ | ✅ | [Reader](docs/plugins/clickhousereader.md) · [Writer](docs/plugins/clickhousewriter.md) |
| | StarRocks | ✅ | ✅ | [Reader](docs/plugins/starrocksreader.md) · [Writer](docs/plugins/starrockswriter.md) |
| | Apache Doris | ✅ | ✅ | [Reader](docs/plugins/dorisreader.md) · [Writer](docs/plugins/doriswriter.md) |
| | HDFS | ✅ | ✅ | [Reader](docs/plugins/hdfsreader.md) · [Writer](docs/plugins/hdfswriter.md) |
| | Databend | ❌ | ✅ | [Writer](docs/plugins/databendwriter.md) |
| **NoSQL Databases** | MongoDB | ✅ | ✅ | [Reader](docs/plugins/mongoreader.md) · [Writer](docs/plugins/mongowriter.md) |
| | Cassandra | ✅ | ✅ | [Reader](docs/plugins/cassandrareader.md) · [Writer](docs/plugins/cassandrawriter.md) |
| | Neo4j | ❌ | ✅ | [Writer](docs/plugins/neo4jwriter.md) |
| | ElasticSearch | ❌ | ✅ | [Writer](docs/plugins/elasticsearchwriter.md) |
| **Time Series Databases** | TDengine | ✅ | ✅ | [Reader](docs/plugins/tdenginereader.md) · [Writer](docs/plugins/tdenginewriter.md) |
| **File Storage** | TXT Files | ✅ | ✅ | [Reader](docs/plugins/txtfilereader.md) · [Writer](docs/plugins/txtfilewriter.md) |
| | JSON Files | ✅ | ✅ | [Reader](docs/plugins/jsonfilereader.md) · [Writer](docs/plugins/jsonfilewriter.md) |
| | FTP/SFTP | ✅ | ✅ | [Reader](docs/plugins/ftpreader.md) · [Writer](docs/plugins/ftpwriter.md) |
| **Cloud Storage** | OSS | ✅ | ❌ | [Reader](docs/plugins/ossreader.md) |
| **Stream Data** | Stream | ✅ | ✅ | [Reader](docs/plugins/streamreader.md) · [Writer](docs/plugins/streamwriter.md) |

## 🚀 Quick Start

### Installation

#### Option 1: Download Prebuilt Binary
```bash
# Linux x86_64
wget https://github.com/longkeyy/go-datax/releases/latest/download/datax-linux-amd64
chmod +x datax-linux-amd64
sudo mv datax-linux-amd64 /usr/local/bin/datax

# macOS Apple Silicon
wget https://github.com/longkeyy/go-datax/releases/latest/download/datax-darwin-arm64
chmod +x datax-darwin-arm64
sudo mv datax-darwin-arm64 /usr/local/bin/datax
```

#### Option 2: Docker Image
```bash
docker pull ghcr.io/longkeyy/go-datax:latest
```

#### Option 3: Build from Source
```bash
git clone https://github.com/longkeyy/go-datax.git
cd go-datax
make build
```

### Basic Usage

1. **Create Configuration File** (`config.json`):
```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 3
      }
    },
    "content": [{
      "reader": {
        "name": "postgresqlreader",
        "parameter": {
          "username": "postgres",
          "password": "password",
          "connection": [{
            "jdbcUrl": ["jdbc:postgresql://localhost:5432/source_db"],
            "table": ["user_table"]
          }],
          "column": ["id", "name", "email"],
          "splitPk": "id"
        }
      },
      "writer": {
        "name": "mysqlwriter",
        "parameter": {
          "username": "root",
          "password": "password",
          "connection": [{
            "jdbcUrl": "jdbc:mysql://localhost:3306/target_db",
            "table": ["user_table"]
          }],
          "column": ["id", "name", "email"]
        }
      }
    }]
  }
}
```

2. **Execute Data Synchronization**:
```bash
# Run locally
./datax -job config.json

# Run with Docker
docker run --rm -v $(pwd)/config.json:/config.json ghcr.io/longkeyy/go-datax:latest -job /config.json
```

### Configuration Parameters

#### Performance Control
```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 4,        // Concurrent channels
        "record": 10000,     // Record limit
        "byte": 1048576      // Byte limit
      },
      "errorLimit": {
        "record": 100,       // Error record threshold
        "percentage": 0.05   // Error rate threshold
      }
    }
  }
}
```

#### Data Filtering and Partitioning
```json
{
  "reader": {
    "parameter": {
      "where": "create_time > '2023-01-01'",
      "splitPk": "user_id",          // Supports numeric, string, and date types
      "fetchSize": 1024
    }
  }
}
```

## 📋 Feature Comparison (vs DataX Java)

### Core Features
- ✅ **Three-Layer Architecture**: Engine-JobContainer-TaskGroupContainer
- ✅ **Plugin System**: Reader/Writer factory pattern registration
- ✅ **Data Model**: Record/Column type system
- ✅ **Configuration Management**: 100% JSON configuration compatibility
- ✅ **Monitoring & Statistics**: Real-time transfer monitoring and performance metrics
- ✅ **Error Control**: errorLimit and fault tolerance mechanisms

### Data Synchronization
- ✅ **Full Sync**: Complete table data synchronization
- ✅ **Incremental Sync**: Business-based incremental sync via WHERE conditions
- ✅ **Heterogeneous Sync**: Cross data source type synchronization
- ✅ **Partitioned Concurrency**: splitPk-based parallel processing
- ❌ **Real-time Sync**: Not supported (consistent with Java version)

### Data Transformation
- ✅ **dx_filter**: Data filtering
- ✅ **dx_substr**: String truncation
- ✅ **dx_replace**: String replacement
- ✅ **dx_pad**: String padding
- ✅ **dx_digest**: Data digest
- ❌ **dx_groovy**: Script transformation (planned)

### Go Version Enhancements
- 🚀 **Non-Numeric splitPk**: Support for string and date type partitioning
- 🚀 **JSON File Support**: JsonFileReader/Writer
- 🚀 **Pure Go Drivers**: Oracle/Sybase without client requirements
- 🚀 **Single Binary**: Zero-dependency deployment

### Data Source Support
- ✅ **Relational Databases** (8): MySQL, PostgreSQL, Oracle, SQL Server, SQLite, OceanBase, GaussDB, Sybase
- ✅ **Big Data Storage** (6): HDFS, ClickHouse, StarRocks, Doris, Databend, TDengine
- ✅ **NoSQL Databases** (4): MongoDB, Cassandra, Neo4j, ElasticSearch
- ✅ **File Storage** (3): TXT Files, JSON Files, FTP/SFTP
- ✅ **Cloud Storage** (1): OSS
- ✅ **Stream Data** (1): Stream
- ❌ **Planned Support**: HBase, MaxCompute, OTS

**Total**: 21 data sources, 42 Reader/Writer plugins

## 📖 Documentation

### User Documentation
- [Quick Start](docs/QUICKSTART.md) - Installation and basic usage guide
- [User Manual](docs/USER_GUIDE.md) - Data synchronization scenarios and configuration details
- [Data Transformation](docs/TRANSFORMER.md) - Built-in Transformer functionality

### Developer Documentation
- [System Architecture](docs/ARCHITECTURE.md) - Go language implementation architecture design
- [Plugin Development](docs/PLUGIN_DEVELOPMENT.md) - Extension plugin development guide
- [Plugin Documentation](docs/plugins/) - Detailed configuration for each plugin

## 🤝 Contributing

Contributions are welcome! Please submit Issues and Pull Requests to help improve the project.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Related Projects

- [DataX (Java)](https://github.com/alibaba/DataX) - Alibaba's open-source heterogeneous data source offline synchronization tool
