# go-datax

golang版本的DataX，完全兼容DataX配置文件格式的数据同步工具。

## 项目特性

✨ **核心优势**
- 🚀 **高性能**: 基于Go语言，支持高并发数据传输
- 🔧 **纯Go实现**: 无需额外依赖，单二进制部署
- 📊 **完整监控**: 实时统计、进度监控、性能指标
- 🔗 **丰富插件**: 支持30+种数据源和目标
- 📝 **结构化日志**: 基于zap的三层日志架构
- ⚡ **智能优化**: 动态缓冲区、批处理、事务管理

## 项目架构

### 核心模块
- **core**: 任务执行引擎、容器管理、调度器
- **common**: 通用组件、接口定义、数据类型、日志系统
- **plugins**: 插件实现
  - **reader**: 数据读取插件
  - **writer**: 数据写入插件

### 设计原理

1. **引擎(Engine)**: 程序入口，负责解析命令行参数、初始化配置
2. **作业容器(JobContainer)**: 负责作业的生命周期管理、任务拆分、调度
3. **任务组容器(TaskGroupContainer)**: 管理并发任务组的执行
4. **插件系统**: Reader和Writer插件，支持多种数据源
5. **统计系统**: 实时数据传输监控和性能统计

### 数据流程

1. 读取配置文件，解析作业参数
2. 初始化Reader和Writer插件
3. 拆分任务，创建多个并发通道
4. 执行数据传输：Reader -> [Transformer] -> Channel -> Writer
5. 监控进度，收集统计信息

## 支持的插件

### 🗄️ 数据库插件

#### Reader插件
- [x] **PostgresqlReader**: 从PostgreSQL数据库读取数据，支持分页、分片
- [x] **MysqlReader**: 从MySQL数据库读取数据，支持主从分离
- [x] **OceanBaseReader**: 从OceanBase分布式数据库读取数据 **(新增)**
  - 🌊 **MySQL协议兼容**: 基于MySQL驱动实现，完全兼容OceanBase
  - 🏢 **租户支持**: 支持OceanBase 1.0/2.0租户格式
  - 🔄 **双兼容模式**: 自动检测MySQL/Oracle兼容模式
  - 📊 **分区优化**: 支持分区表并行读取优化
  - 💡 **弱一致性**: 支持弱一致性读取提升性能
  - ⚖️ **负载均衡**: 支持OceanBase集群负载均衡
- [x] **GaussDBReader**: 从华为GaussDB数据库读取数据 **(新增)**
  - 🔧 **PostgreSQL协议**: 基于PostgreSQL驱动，兼容openGauss内核
  - 🏢 **企业级**: 支持华为企业级数据库完整特性
  - 🎯 **专用优化**: 针对GaussDB特殊数据类型优化
  - 📊 **高性能**: 支持分布式架构和分片读取
- [x] **SqliteReader**: 从SQLite数据库读取数据，支持ROWID优化
- [x] **SqlServerReader**: 从SQL Server数据库读取数据
- [x] **OracleReader**: 从Oracle数据库读取数据 **(基于go-ora纯Go驱动)**
- [x] **SybaseReader**: 从Sybase ASE数据库读取数据 **(新增)**
  - 🔧 **TDS协议**: 基于thda/tds纯Go驱动，原生TDS协议支持
  - 🏢 **企业级**: 支持Sybase ASE企业级数据库完整特性
  - 🎯 **兼容性**: 完全兼容DataX Java版本配置参数
  - 📊 **高性能**: 支持fetchSize优化和分片读取
  - 🔄 **纯Go**: 无需安装额外驱动，单二进制部署
- [x] **CassandraReader**: 从Apache Cassandra分布式数据库读取数据 **(新增)**
- [x] **ClickHouseReader**: 从ClickHouse列存数据库读取数据
- [x] **DorisReader**: 从Apache Doris数据库读取数据
- [x] **StarRocksReader**: 从StarRocks分析数据库读取数据 **(新增)**
- [x] **TDengineReader**: 从TDengine时序数据库读取数据 **(新增)**

#### Writer插件
- [x] **PostgresqlWriter**: 向PostgreSQL数据库写入数据，支持批量写入
- [x] **MysqlWriter**: 向MySQL数据库写入数据，支持事务批处理
- [x] **OceanBaseWriter**: 向OceanBase分布式数据库写入数据 **(新增)**
  - 🌊 **MySQL协议兼容**: 基于MySQL驱动实现，完全兼容OceanBase
  - 🏢 **租户支持**: 支持OceanBase 1.0/2.0租户格式
  - 🔄 **双兼容模式**: 自动检测MySQL/Oracle兼容模式
  - ⚡ **批量优化**: 支持大批量高性能写入
  - 🎯 **写入模式**: 支持insert/replace/update多种写入模式
  - 📋 **关键字转义**: 根据兼容模式自动转义关键字
- [x] **GaussDBWriter**: 向华为GaussDB数据库写入数据 **(新增)**
  - 🔧 **PostgreSQL协议**: 基于PostgreSQL驱动，兼容openGauss内核
  - ⚡ **仅INSERT模式**: 按Java版本规范，仅支持INSERT写入模式
  - 🎯 **类型转换**: 支持serial、bigserial、bit等特殊类型转换
  - 📊 **批量事务**: 事务批量写入保证数据一致性
- [x] **SqliteWriter**: 向SQLite数据库写入数据
- [x] **SqlServerWriter**: 向SQL Server数据库写入数据
- [x] **OracleWriter**: 向Oracle数据库写入数据 **(基于go-ora纯Go驱动)**
- [x] **SybaseWriter**: 向Sybase ASE数据库写入数据 **(新增)**
  - 🔧 **TDS协议**: 基于thda/tds纯Go驱动，原生TDS协议支持
  - ⚡ **批量写入**: 支持高性能批量插入和事务处理
  - 🎯 **写入模式**: 支持insert/replace/update多种写入模式
  - 🔄 **故障恢复**: 支持replace模式的失败恢复机制
  - 📊 **类型支持**: 支持Sybase ASE全部数据类型转换
- [x] **CassandraWriter**: 向Apache Cassandra分布式数据库写入数据 **(新增)**
- [x] **ClickHouseWriter**: 向ClickHouse列存数据库写入数据
- [x] **DatabendWriter**: 向Databend云原生数据仓库写入数据 **(新增)**
  - ☁️ **云原生架构**: 基于官方databend-go驱动，支持Databend Cloud和开源版本
  - 🚀 **高性能写入**: 支持批量写入和流式写入，内置S3 stage优化
  - 🔄 **写入模式**: 支持insert和replace两种写入模式
  - 🎯 **冲突处理**: 支持onConflictColumn配置处理数据冲突
  - 📊 **类型支持**: 支持Databend全部数据类型，包括variant复杂类型
  - ⚡ **预处理**: 支持preSql/postSql执行前后处理逻辑
  - 🔐 **认证安全**: 支持用户名密码认证和连接池管理
- [x] **DorisWriter**: 向Apache Doris数据库写入数据
- [x] **StarRocksWriter**: 向StarRocks分析数据库写入数据，支持Stream Load **(新增)**
- [x] **TDengineWriter**: 向TDengine时序数据库写入数据 **(新增)**
- [x] **Neo4jWriter**: 向Neo4j图数据库写入数据 **(新增)**
  - 🔗 **图数据库**: 专为Neo4j 4.x/5.x优化的图数据写入
  - 📊 **批量写入**: 使用UNWIND语法实现高性能批量导入
  - 🎯 **多种认证**: 支持基本认证、Bearer Token、Kerberos认证
  - 🔄 **事务重试**: 内置重试机制和事务超时控制
  - 📝 **类型丰富**: 支持Neo4j全部数据类型，包括数组、Map、日期时间
  - ⚡ **Cypher灵活**: 支持自定义Cypher语句，可创建节点和关系

### 📄 文件插件

#### Reader插件
- [x] **TxtFileReader**: 读取文本文件，支持CSV、TSV等格式
- [x] **JsonFileReader**: 读取JSON/JSONL文件 **(新增)**
  - 🔍 **智能模式检测**: 自动识别JSON vs JSONL格式
  - 📋 **模式推断**: 预读100行自动推断字段类型
  - 🌐 **Glob支持**: 支持`/*/*.jsonl`等通配符模式
  - 🗂️ **递归遍历**: 支持目录递归扫描
- [x] **FtpReader**: 从FTP/SFTP服务器读取文本文件 **(新增)**
  - 🌐 **双协议支持**: 同时支持FTP和SFTP协议
  - 📁 **路径通配**: 支持通配符和目录递归遍历
  - 🔐 **安全连接**: SFTP支持SSH密钥和密码认证
  - 📊 **格式兼容**: 继承TxtFileReader的所有文件格式特性
- [x] **OssReader**: 从阿里云OSS对象存储读取文件 **(新增)**
  - ☁️ **原生OSS SDK**: 基于阿里云官方SDK v2实现
  - 🎯 **通配符支持**: 支持*和?通配符匹配对象
  - 📊 **智能分片**: 大文件自动分片并行读取
  - 🗂️ **多格式支持**: 支持文本和二进制文件读取
  - 🔐 **完整认证**: 支持AccessKey和代理配置
  - 📈 **容错处理**: 支持successOnNoObject容错模式

#### Writer插件
- [x] **TxtFileWriter**: 写入文本文件，支持多种分隔符
- [x] **JsonFileWriter**: 写入JSON/JSONL文件 **(新增)**
  - 📝 **JSONL优化**: 默认JSONL格式避免内存问题
  - ✂️ **智能分片**: 支持按文件大小和记录数自动分片
  - 🎯 **类型保持**: 保持原始数据类型（数字、布尔、日期）
- [x] **FtpWriter**: 向FTP/SFTP服务器写入文本文件 **(新增)**
  - 🌐 **双协议支持**: 同时支持FTP和SFTP协议
  - 📁 **智能目录**: 自动创建递归目录结构
  - 🔄 **写入模式**: 支持truncate、append、nonConflict模式
  - 💾 **压缩支持**: 支持gzip压缩传输
- [x] **OssWriter**: 向阿里云OSS对象存储写入文件 **(新增)**
  - ☁️ **原生OSS SDK**: 基于阿里云官方SDK v2实现
  - 🚀 **分片上传**: 支持大文件多块并行上传
  - 📝 **多写入模式**: 支持单对象/多对象写入策略
  - 🔄 **写入策略**: 支持truncate、append、nonConflict模式
  - 🗂️ **多格式支持**: 支持文本和二进制文件写入
  - 📦 **文件轮转**: 支持按大小自动分割文件
  - 🔐 **服务端加密**: 支持OSS服务端加密存储

### 🏪 NoSQL插件

#### Reader/Writer插件
- [x] **MongoReader/MongoWriter**: MongoDB文档数据库支持
- [x] **OtsReader/OtsWriter**: 阿里云TableStore(OTS)表格存储支持 **(新增)**
  - ☁️ **官方SDK**: 基于阿里云官方TableStore Go SDK实现
  - 📊 **多种模式**: 支持normal、multiVersion多版本读取模式
  - 🎯 **范围查询**: 支持主键范围查询和自动分片
  - ⏰ **时序支持**: 支持时序表(Timeseries)数据读写
  - 🔄 **写入策略**: 支持PUT、UPDATE等多种写入模式
  - 📈 **批量操作**: 支持批量读写优化性能
  - 🔐 **完整认证**: 支持AccessKey认证和实例配置
  - 💾 **类型兼容**: 支持string、int、binary、double、bool等数据类型
  - ⚡ **性能优化**: 连接池、重试机制、超时控制
- [x] **StreamReader/StreamWriter**: 流式数据处理

### 🔍 搜索引擎插件

#### Writer插件
- [x] **ElasticSearchWriter**: 向ElasticSearch搜索引擎写入数据 **(新增)**
  - 🔄 **版本兼容**: 支持ElasticSearch 5.x, 6.x, 7.x, 8.x
  - ⚡ **批量写入**: 高性能bulk操作
  - 🏗️ **索引管理**: 自动创建索引和映射
  - 🆔 **ID策略**: 支持自动ID、组合ID、主键ID
  - 📊 **动态索引**: 支持时间格式动态索引名
  - 🗑️ **清理模式**: 支持truncate清理已有数据
  - 🏷️ **别名管理**: 支持索引别名创建和管理
  - 🎯 **操作类型**: 支持index、create、update、delete操作
  - 🔀 **路由策略**: 支持分区路由优化
  - ❌ **错误处理**: 支持忽略写入错误和解析错误

### 🏔️ 大数据生态插件

#### Reader/Writer插件
- [x] **HdfsReader/HdfsWriter**: Hadoop分布式文件系统支持 **(新增)**
  - 🗄️ **多格式支持**: TEXT、CSV、ORC、Parquet、Sequence、RC文件格式
  - 🔐 **Kerberos认证**: 支持安全集群访问
  - 📁 **路径通配**: 支持通配符路径匹配和目录遍历
  - 🎯 **分片读取**: 智能文件分片，支持高并发读取
  - 📊 **压缩支持**: 支持GZIP、BZIP2等压缩格式
  - 🔄 **写入模式**: 支持append、truncate、nonConflict写入模式
  - ⚡ **高性能**: 基于go-hdfs库，原生Go实现

### ☁️ 云存储插件

#### Reader/Writer插件
- [x] **OssReader/OssWriter**: 阿里云对象存储OSS支持 **(新增)**
  - ☁️ **官方SDK**: 基于阿里云官方OSS SDK v2实现
  - 🎯 **对象匹配**: 支持通配符和正则表达式匹配OSS对象
  - 🚀 **分片传输**: 大文件自动分片并行上传/下载
  - 📝 **多写入模式**: 支持单对象和多对象写入策略
  - 🔄 **操作模式**: 支持truncate、append、nonConflict写入模式
  - 🗂️ **格式兼容**: 支持文本、二进制、压缩文件格式
  - 🔐 **完整认证**: 支持AccessKey、CNAME、代理等配置
  - 📈 **容错处理**: 支持successOnNoObject等容错模式
  - 🔒 **安全特性**: 支持服务端加密和访问控制

## 🚀 新功能亮点

### Hadoop HDFS分布式文件系统支持
- ✅ **完整兼容**: 与DataX Java版本配置100%兼容
- ✅ **多格式支持**: TEXT、CSV、ORC、Parquet、Sequence、RC文件格式（当前实现TEXT格式）
- ✅ **分布式架构**: 支持HDFS集群多节点访问
- ✅ **智能分片**: 每个文件独立分片，支持高并发并行处理
- ✅ **路径处理**: 支持通配符、目录递归、绝对路径验证
- ✅ **安全认证**: 预留Kerberos认证接口，支持企业级安全
- ✅ **写入优化**: 临时文件机制，原子性写入操作
- ✅ **压缩支持**: 支持GZIP、BZIP2文件压缩
- ✅ **纯Go实现**: 基于colinmarc/hdfs/v2库，无外部依赖

### 阿里云OSS对象存储支持
- ✅ **完整兼容**: 与DataX Java版本配置100%兼容
- ✅ **官方SDK**: 基于阿里云官方OSS SDK v2实现
- ✅ **智能分片**: 大文件自动分片并行传输，支持GB级文件
- ✅ **对象匹配**: 支持通配符（*、?）和正则表达式匹配
- ✅ **多种模式**: 支持单对象/多对象写入、分片上传/下载
- ✅ **格式兼容**: 支持文本、二进制、压缩文件等多种格式
- ✅ **容错机制**: 支持successOnNoObject、错误重试等容错处理
- ✅ **安全特性**: 支持AccessKey认证、CNAME、代理、服务端加密
- ✅ **写入策略**: 支持truncate、append、nonConflict等操作模式

### Apache Cassandra数据库支持
- ✅ **分布式架构**: 支持Cassandra集群多节点连接
- ✅ **数据分片**: 智能token范围分割，支持RandomPartitioner和Murmur3Partitioner
- ✅ **全类型支持**: Text、Int、UUID、Timestamp、Collection等复杂类型
- ✅ **性能优化**: 批量写入、异步操作、连接池管理
- ✅ **一致性控制**: 支持多级一致性配置（ONE、QUORUM、ALL等）
- ✅ **SSL支持**: 安全连接加密传输

### Oracle数据库支持（基于go-ora）
- ✅ **纯Go实现**: 无需安装Oracle客户端库
- ✅ **简化部署**: 单二进制包含所有依赖
- ✅ **全类型支持**: NUMBER、VARCHAR2、DATE、CLOB、BLOB等
- ✅ **高性能**: 批量插入、事务处理、连接池
### Neo4j图数据库支持
- ✅ **图数据库专业**: 支持Neo4j 4.x和5.x版本，专为图数据优化
- ✅ **高性能批量导入**: 使用UNWIND语法实现批量写入，支持10万+级别数据
- ✅ **多种认证方式**: 支持基本认证(用户名/密码)、Bearer Token、Kerberos认证
- ✅ **事务可靠性**: 内置重试机制、事务超时控制、死锁检测
- ✅ **丰富数据类型**: 支持Neo4j全部数据类型，包括基础类型、数组、Map、日期时间
- ✅ **灵活Cypher语句**: 支持自定义Cypher语句，可创建节点、关系、复杂图结构
- ✅ **批量优化**: 可配置批量大小、连接池、重试策略，优化大数据量场景
- ✅ **完全兼容**: 与DataX Java版本配置100%兼容，平滑迁移

### JSON文件处理
- ✅ **智能推断**: 自动检测字段类型（long、double、boolean、date、string）
- ✅ **格式兼容**: 同时支持标准JSON和行分隔JSON（JSONL）
- ✅ **模式灵活**: 支持嵌套对象、数组、复杂类型
- ✅ **性能优化**: 流式处理大文件，内存占用可控

### FTP/SFTP文件传输支持
- ✅ **双协议支持**: 同时支持FTP和SFTP协议，与Java版本完全兼容
- ✅ **安全认证**: SFTP支持SSH密码认证，自动处理主机密钥
- ✅ **连接管理**: 内置重试机制，支持超时和连接模式配置
- ✅ **路径处理**: 支持通配符路径匹配和递归目录遍历
- ✅ **文件操作**: 自动创建目录结构，支持多种写入模式
- ✅ **数据格式**: 继承所有文本文件格式特性（CSV、压缩、编码等）

### 结构化日志系统
- ✅ **三层架构**: APP（应用级）、COMPONENT（组件级）、TASK（任务级）
- ✅ **上下文感知**: 自动附加任务组ID、组件名称
- ✅ **性能监控**: 详细的传输速度、错误统计
- ✅ **问题诊断**: 结构化错误信息，便于故障排查

## 配置文件格式

兼容DataX原生JSON配置格式：

### 基础配置示例

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 5
      }
    },
    "content": [
      {
        "reader": {
          "name": "postgresqlreader",
          "parameter": {
            "username": "postgres",
            "password": "postgres",
            "connection": [
              {
                "jdbcUrl": ["jdbc:postgresql://localhost:5432/source_db"],
                "table": ["users"]
              }
            ],
            "column": ["*"]
          }
        },
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "username": "root",
            "password": "password",
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://localhost:3306/target_db?charset=utf8",
                "table": ["users"]
              }
            ],
            "column": ["*"],
            "writeMode": "insert"
          }
        }
      }
    ]
  }
}
```

## 使用方法

### 编译和运行

```bash
# 编译
make build

# 或直接运行
go run cmd/datax/main.go -job config.json

# 使用编译后的二进制
./datax -job config.json
```

### 配置示例

#### Oracle数据库配置 🆕

```json
{
  "reader": {
    "name": "oraclereader",
    "parameter": {
      "username": "scott",
      "password": "tiger",
      "connection": [
        {
          "jdbcUrl": ["oracle://localhost:1521/xe"],
          "table": ["emp"]
        }
      ],
      "column": ["*"],
      "splitPk": "id"
    }
  },
  "writer": {
    "name": "oraclewriter",
    "parameter": {
      "username": "scott",
      "password": "tiger",
      "connection": [
        {
          "jdbcUrl": "oracle://localhost:1521/xe",
          "table": ["emp_copy"]
        }
      ],
      "column": ["*"],
      "writeMode": "INSERT"
    }
  }
}
```

#### GaussDB数据库配置 🆕

```json
{
  "reader": {
    "name": "gaussdbreader",
    "parameter": {
      "username": "gaussdb",
      "password": "Gauss_234",
      "connection": [
        {
          "jdbcUrl": ["jdbc:opengauss://localhost:5432/testdb"],
          "table": ["users"]
        }
      ],
      "column": ["*"],
      "splitPk": "id",
      "fetchSize": 1000
    }
  },
  "writer": {
    "name": "gaussdbwriter",
    "parameter": {
      "username": "gaussdb",
      "password": "Gauss_234",
      "connection": [
        {
          "jdbcUrl": "jdbc:gaussdb://localhost:5432/testdb",
          "table": ["users_copy"]
        }
      ],
      "column": ["*"],
      "batchSize": 1024
    }
  }
}
```

#### HDFS分布式文件系统配置 🆕

```json
{
  "reader": {
    "name": "hdfsreader",
    "parameter": {
      "defaultFS": "hdfs://namenode:9000",
      "path": ["/user/data/*.txt"],
      "fileType": "TEXT",
      "encoding": "UTF-8",
      "fieldDelimiter": ",",
      "column": [
        {"index": 0, "type": "string"},
        {"index": 1, "type": "long"},
        {"index": 2, "type": "double"},
        {"index": 3, "type": "date", "format": "yyyy-MM-dd"}
      ],
      "haveKerberos": false,
      "hdfsUsername": "hadoop"
    }
  },
  "writer": {
    "name": "hdfswriter",
    "parameter": {
      "defaultFS": "hdfs://namenode:9000",
      "path": "/user/output",
      "fileName": "output_data",
      "fileType": "TEXT",
      "fieldDelimiter": "\t",
      "writeMode": "truncate",
      "encoding": "UTF-8",
      "column": [
        {"name": "id", "type": "string"},
        {"name": "count", "type": "long"},
        {"name": "amount", "type": "double"},
        {"name": "create_time", "type": "date"}
      ],
      "compress": "GZIP",
      "haveKerberos": false,
      "hdfsUsername": "hadoop"
    }
  }
}
```

#### JSON文件处理配置 🆕

```json
{
  "reader": {
    "name": "jsonfilereader",
    "parameter": {
      "path": ["/data/logs/**/*.jsonl"],
      "format": "auto",
      "encoding": "UTF-8"
    }
  },
  "writer": {
    "name": "jsonfilewriter",
    "parameter": {
      "path": "/output/processed_data.jsonl",
      "format": "jsonl",
      "maxFileSize": "100MB",
      "maxRecordsPerFile": 1000000,
      "encoding": "UTF-8"
    }
  }
}
```

#### 数据库连接配置

##### PostgreSQL
```json
{
  "reader": {
    "name": "postgresqlreader",
    "parameter": {
      "username": "postgres",
      "password": "postgres",
      "connection": [
        {
          "jdbcUrl": ["jdbc:postgresql://localhost:5432/source_db"],
          "table": ["table_name"]
        }
      ],
      "column": ["*"],
      "splitPk": "id",
      "where": "created_at >= '2023-01-01'"
    }
  }
}
```

##### MySQL
```json
{
  "reader": {
    "name": "mysqlreader",
    "parameter": {
      "username": "root",
      "password": "password",
      "connection": [
        {
          "jdbcUrl": ["jdbc:mysql://localhost:3306/source_db?charset=utf8mb4"],
          "table": ["table_name"]
        }
      ],
      "column": ["*"],
      "splitPk": "id"
    }
  }
}
```

##### ClickHouse
```json
{
  "reader": {
    "name": "clickhousereader",
    "parameter": {
      "username": "default",
      "password": "",
      "connection": [
        {
          "jdbcUrl": ["clickhouse://localhost:9000/default"],
          "table": ["table_name"]
        }
      ],
      "column": ["*"]
    }
  }
}
```

##### StarRocks（分析型数据库）🆕
```json
{
  "reader": {
    "name": "starrocksreader",
    "parameter": {
      "username": "root",
      "password": "password",
      "connection": [
        {
          "jdbcUrl": ["jdbc:mysql://starrocks-fe:9030/demo_db"],
          "table": ["user_behavior"]
        }
      ],
      "column": ["user_id", "item_id", "category_id", "behavior_type", "timestamp"],
      "where": "timestamp >= '2023-01-01'",
      "splitPk": "user_id"
    }
  },
  "writer": {
    "name": "starrockswriter",
    "parameter": {
      "username": "root",
      "password": "password",
      "database": "analytics_db",
      "connection": [
        {
          "jdbcUrl": ["jdbc:mysql://starrocks-fe:9030/analytics_db"],
          "table": ["user_behavior_analysis"]
        }
      ],
      "column": ["user_id", "item_id", "category_id", "behavior_type", "timestamp"],
      "loadUrl": ["starrocks-fe:8030", "starrocks-fe2:8030"],
      "labelPrefix": "datax_",
      "maxBatchRows": 100000,
      "maxBatchSize": 10485760,
      "flushInterval": 30000,
      "loadProps": {
        "format": "CSV",
        "column_separator": "\t",
        "row_delimiter": "\n",
        "timeout": 3600
      }
    }
  }
}
```

## 技术栈

### 核心依赖
- **Go 1.23+**: 现代Go语言特性
- **Zap**: 高性能结构化日志
- **Context**: 上下文传播和取消

### 数据库驱动
- **PostgreSQL**: `github.com/jackc/pgx/v5` - 高性能原生驱动
- **MySQL**: `github.com/go-sql-driver/mysql` - 官方推荐驱动
- **SQLite**: `github.com/mattn/go-sqlite3` - CGO实现
- **SQL Server**: `github.com/microsoft/go-mssqldb` - 微软官方驱动
- **Oracle**: `github.com/sijms/go-ora/v2` - 纯Go实现，无需客户端 🆕
- **GaussDB**: 基于PostgreSQL驱动，兼容openGauss和华为GaussDB 🆕
- **ClickHouse**: `github.com/ClickHouse/clickhouse-go/v2` - 官方驱动
- **StarRocks**: 基于MySQL驱动，兼容MySQL协议，支持Stream Load 🆕
- **TDengine**: `github.com/taosdata/driver-go/v3` - 官方时序数据库驱动
- **MongoDB**: `go.mongodb.org/mongo-driver` - 官方驱动

### 大数据生态驱动
- **HDFS**: `github.com/colinmarc/hdfs/v2` - 纯Go实现的HDFS客户端，支持Kerberos认证 🆕

### 云存储驱动
- **阿里云OSS**: `github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss` - 阿里云官方OSS SDK v2 🆕

### ORM框架
- **GORM**: `gorm.io/gorm` - 现代化ORM，支持多数据库

## 性能特性

### 🚀 传输优化
- **动态缓冲区**: 根据数据量自动调整Channel缓冲区大小
- **批量处理**: 支持批量插入，减少网络往返
- **事务管理**: 智能事务边界，保证数据一致性
- **并发控制**: 多Channel并发传输，提升吞吐量

### 📊 监控统计
- **实时进度**: 传输速度、剩余时间估算
- **详细指标**: 成功/失败记录数、字节传输量
- **错误统计**: 分类错误统计和错误率监控
- **性能分析**: Transformer执行时间、数据库操作耗时

### 🔍 可观测性
- **结构化日志**: JSON格式，便于日志收集和分析
- **链路追踪**: 任务组级别的上下文传播
- **问题定位**: 详细的错误堆栈和上下文信息

## 开发规范

### 编译构建
```bash
# 使用Makefile编译
make build

# 清理构建产物
make clean

# 运行测试
make test
```

### 日志规范
```go
// 应用级日志
logger.App().Info("Application started")

// 组件级日志
logger.Component().WithComponent("MySQLReader").Error("Connection failed", zap.Error(err))

// 任务级日志
taskLogger := logger.TaskGroup(taskGroupId)
taskLogger.Info("Task completed", zap.Int64("records", count))
```

### 配置规范
- 遵循DataX原生配置格式
- 支持JDBC URL格式的数据库连接
- 参数验证和默认值处理
- 错误提示友好化

## 更新日志

### v1.4.0 (最新) 🆕
- **新增**: Apache Cassandra分布式数据库支持（基于官方gocql驱动）
- **新增**: FTP/SFTP文件传输协议支持（支持双协议）
- **新增**: Hadoop HDFS分布式文件系统支持（基于纯Go实现）
- **新增**: ElasticSearch搜索引擎支持（兼容5.x-8.x版本）
- **新增**: Neo4j图数据库支持（专业图数据写入）
- **新增**: Databend云原生数据仓库支持
- **新增**: StarRocks分析数据库支持（兼容MySQL协议，支持Stream Load）
- **新增**: OceanBase分布式数据库支持（兼容MySQL/Oracle双模式）
- **新增**: GaussDB企业级数据库支持（基于PostgreSQL协议）
- **新增**: Sybase ASE数据库支持（基于纯Go TDS协议）
- **新增**: TDengine时序数据库支持（基于官方驱动）
- **优化**: 完成所有新插件的Java版本兼容性验证
- **优化**: 项目现在支持30+种数据库和存储系统

### v1.3.0
- **新增**: 阿里云OSS对象存储支持（基于官方SDK v2）
- **新增**: Hadoop HDFS分布式文件系统支持
- **新增**: Oracle数据库支持（基于go-ora纯Go驱动）
- **新增**: StarRocks分析数据库支持（兼容MySQL协议，支持Stream Load）
- **新增**: JSON/JSONL文件处理插件
- **优化**: 完成zap结构化日志迁移
- **优化**: 动态缓冲区大小调整
- **修复**: 多项编译错误和API兼容性问题

### v1.2.0
- **新增**: ClickHouse、Doris数据库支持
- **新增**: MongoDB文档数据库支持
- **优化**: 批量处理性能提升
- **优化**: 错误处理和重试机制

### v1.1.0
- **新增**: SQL Server数据库支持
- **新增**: 文本文件处理插件
- **优化**: 并发性能和内存管理
- **修复**: 数据类型转换问题

### v1.0.0
- **核心**: 基础架构和引擎实现
- **支持**: PostgreSQL、MySQL、OceanBase、SQLite、TDengine数据库
- **特性**: 多通道并发、实时监控

## 贡献指南

1. **文档先行**: 任何代码变更前必须更新相关文档
2. **规范提交**: 遵循Conventional Commits规范
3. **测试完整**: 新功能必须包含测试用例
4. **架构统一**: 遵循项目架构模式和命名规范

## 许可证

本项目采用开源许可证发布，详见LICENSE文件。

---

**注意**: 本项目正在积极开发中，功能和API可能会有变化。生产环境使用前请充分测试。