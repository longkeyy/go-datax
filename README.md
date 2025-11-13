[English](README_EN.md) | 简体中文

# go-datax

Go版本的DataX数据同步工具，100%兼容DataX JSON配置格式。

## ✨ 核心特性

- 🚀 **高性能**: Go语言实现，高并发数据传输
- 🔧 **零依赖**: 单二进制文件，无需额外安装
- 📊 **30+数据源**: 支持主流数据库和存储系统
- ✅ **100%兼容**: 完全兼容DataX Java版配置文件
- ⚡ **Go优化**: 增强的splitPk支持和JSON文件处理

## 🆕 Go版本增强功能

### 非数字类型splitPk支持
与Java版本只支持数字类型splitPk不同，Go版本支持：
- **数字类型**: `int`, `bigint`, `decimal`等（与Java版本兼容）
- **字符串类型**: `varchar`, `text`等字符串字段分片
- **日期类型**: `date`, `timestamp`等时间字段分片

```json
{
  "reader": {
    "parameter": {
      "splitPk": "create_time",  // 支持日期时间字段分片
      "where": "status = 'active'"
    }
  }
}
```

### JSON文件数据源支持
新增专用JSON文件读取和写入插件：
- **JsonFileReader**: 读取JSON/JSONL文件
- **JsonFileWriter**: 写入JSON格式文件
- **自动模式识别**: 自动检测标准JSON vs JSONL格式

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

## 🏗️ 架构设计

采用DataX三层架构设计：

```
Engine Layer (引擎层)
├── JobContainer (作业容器)
└── TaskGroupContainer (任务组容器)
    ├── Reader Task (读取任务)
    ├── Channel (数据通道)
    └── Writer Task (写入任务)
```

## 📦 支持的数据源

DataX目前已经有了比较全面的插件体系，主流的RDBMS数据库、NOSQL、大数据计算系统都已经接入。

| 类型 | 数据源 | Reader(读) | Writer(写) | 文档 |
|------|--------|:---------:|:---------:|------|
| **关系型数据库** | MySQL | ✅ | ✅ | [读](docs/plugins/mysqlreader.md) 、[写](docs/plugins/mysqlwriter.md) |
| | PostgreSQL | ✅ | ✅ | [读](docs/plugins/postgresqlreader.md) 、[写](docs/plugins/postgresqlwriter.md) |
| | Oracle | ✅ | ✅ | [读](docs/plugins/oraclereader.md) 、[写](docs/plugins/oraclewriter.md) |
| | SQL Server | ✅ | ✅ | [读](docs/plugins/sqlserverreader.md) 、[写](docs/plugins/sqlserverwriter.md) |
| | SQLite | ✅ | ✅ | [读](docs/plugins/sqlitereader.md) 、[写](docs/plugins/sqlitewriter.md) |
| | OceanBase | ✅ | ✅ | [读](docs/plugins/oceanbasereader.md) 、[写](docs/plugins/oceanbasewriter.md) |
| | GaussDB | ✅ | ✅ | [读](docs/plugins/gaussdbreader.md) 、[写](docs/plugins/gaussdbwriter.md) |
| | Sybase ASE | ✅ | ✅ | [读](docs/plugins/sybasereader.md) 、[写](docs/plugins/sybasewriter.md) |
| **大数据存储** | ClickHouse | ✅ | ✅ | [读](docs/plugins/clickhousereader.md) 、[写](docs/plugins/clickhousewriter.md) |
| | StarRocks | ✅ | ✅ | [读](docs/plugins/starrocksreader.md) 、[写](docs/plugins/starrockswriter.md) |
| | Apache Doris | ✅ | ✅ | [读](docs/plugins/dorisreader.md) 、[写](docs/plugins/doriswriter.md) |
| | HDFS | ✅ | ✅ | [读](docs/plugins/hdfsreader.md) 、[写](docs/plugins/hdfswriter.md) |
| | Databend | ❌ | ✅ | [写](docs/plugins/databendwriter.md) |
| **NoSQL数据库** | MongoDB | ✅ | ✅ | [读](docs/plugins/mongoreader.md) 、[写](docs/plugins/mongowriter.md) |
| | Cassandra | ✅ | ✅ | [读](docs/plugins/cassandrareader.md) 、[写](docs/plugins/cassandrawriter.md) |
| | Neo4j | ❌ | ✅ | [写](docs/plugins/neo4jwriter.md) |
| | ElasticSearch | ❌ | ✅ | [写](docs/plugins/elasticsearchwriter.md) |
| **时序数据库** | TDengine | ✅ | ✅ | [读](docs/plugins/tdenginereader.md) 、[写](docs/plugins/tdenginewriter.md) |
| **文件存储** | TXT文件 | ✅ | ✅ | [读](docs/plugins/txtfilereader.md) 、[写](docs/plugins/txtfilewriter.md) |
| | JSON文件 | ✅ | ✅ | [读](docs/plugins/jsonfilereader.md) 、[写](docs/plugins/jsonfilewriter.md) |
| | FTP/SFTP | ✅ | ✅ | [读](docs/plugins/ftpreader.md) 、[写](docs/plugins/ftpwriter.md) |
| **云存储** | OSS | ✅ | ❌ | [读](docs/plugins/ossreader.md) |
| **流数据** | Stream | ✅ | ✅ | [读](docs/plugins/streamreader.md) 、[写](docs/plugins/streamwriter.md) |

## 🚀 快速开始

### 安装

#### 方式1: 下载预编译二进制
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

#### 方式2: Docker镜像
```bash
docker pull ghcr.io/longkeyy/go-datax:latest
```

#### 方式3: 源码编译
```bash
git clone https://github.com/longkeyy/go-datax.git
cd go-datax
make build
```

### 基础使用

1. **创建配置文件** (`config.json`):
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

2. **执行数据同步**:
```bash
# 本地运行
./datax -job config.json

# Docker运行
docker run --rm -v $(pwd)/config.json:/config.json ghcr.io/longkeyy/go-datax:latest -job /config.json
```

### 配置参数说明

#### 性能控制
```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 4,        // 并发通道数
        "record": 10000,     // 记录数限制
        "byte": 1048576      // 字节数限制
      },
      "errorLimit": {
        "record": 100,       // 错误记录上限
        "percentage": 0.05   // 错误率上限
      }
    }
  }
}
```

#### 数据过滤和分片
```json
{
  "reader": {
    "parameter": {
      "where": "create_time > '2023-01-01'",
      "splitPk": "user_id",          // 支持数字、字符串、日期类型
      "fetchSize": 1024
    }
  }
}
```

## 📋 功能对比 (vs DataX Java版)

### 核心功能
- ✅ **三层架构**: Engine-JobContainer-TaskGroupContainer
- ✅ **插件系统**: Reader/Writer工厂模式注册
- ✅ **数据模型**: Record/Column类型系统
- ✅ **配置管理**: 100% JSON配置兼容
- ✅ **监控统计**: 实时传输监控和性能指标
- ✅ **错误控制**: errorLimit和容错机制

### 数据同步
- ✅ **全量同步**: 完整表数据同步
- ✅ **增量同步**: 基于WHERE条件的业务增量
- ✅ **异构同步**: 跨数据源类型同步
- ✅ **分片并发**: splitPk分片并行处理
- ❌ **实时同步**: 不支持（与Java版一致）

### 数据转换
- ✅ **dx_filter**: 数据过滤
- ✅ **dx_substr**: 字符串截取
- ✅ **dx_replace**: 字符串替换
- ✅ **dx_pad**: 字符串填充
- ✅ **dx_digest**: 数据摘要
- ❌ **dx_groovy**: 脚本转换（计划支持）

### Go版本增强
- 🚀 **非数字splitPk**: 支持字符串、日期类型分片
- 🚀 **JSON文件支持**: JsonFileReader/Writer
- 🚀 **纯Go驱动**: Oracle/Sybase无需客户端
- 🚀 **单二进制**: 零依赖部署

### 数据源支持
- ✅ **关系型数据库** (8个): MySQL, PostgreSQL, Oracle, SQL Server, SQLite, OceanBase, GaussDB, Sybase
- ✅ **大数据存储** (6个): HDFS, ClickHouse, StarRocks, Doris, Databend, TDengine
- ✅ **NoSQL数据库** (4个): MongoDB, Cassandra, Neo4j, ElasticSearch
- ✅ **文件存储** (3个): TXT文件, JSON文件, FTP/SFTP
- ✅ **云存储** (1个): OSS
- ✅ **流数据** (1个): Stream
- ❌ **计划支持**: HBase, MaxCompute, OTS

**总计**: 21种数据源，42个Reader/Writer插件

## 📖 文档

### 用户文档
- [快速开始](docs/QUICKSTART.md) - 安装和基础使用指南
- [用户手册](docs/USER_GUIDE.md) - 数据同步场景和配置详解
- [数据转换](docs/TRANSFORMER.md) - 内置Transformer功能说明

### 开发者文档
- [系统架构](docs/ARCHITECTURE.md) - Go语言实现架构设计
- [插件开发](docs/PLUGIN_DEVELOPMENT.md) - 扩展插件开发指南
- [插件文档](docs/plugins/) - 各插件详细配置说明

## 🤝 贡献

欢迎提交Issue和Pull Request来帮助改进项目。

## 📄 许可证

本项目采用MIT许可证 - 详见[LICENSE](LICENSE)文件。

## 🔗 相关项目

- [DataX (Java版)](https://github.com/alibaba/DataX) - 阿里巴巴开源的异构数据源离线同步工具