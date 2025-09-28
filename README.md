# go-datax

golang版本的DataX，完全兼容DataX配置文件格式的数据同步工具。

## 项目特性

✨ **核心优势**
- 🚀 **高性能**: 基于Go语言，支持高并发数据传输
- 🔧 **纯Go实现**: 无需额外依赖，单二进制部署
- 📊 **完整监控**: 实时统计、进度监控、性能指标
- 🔗 **丰富插件**: 支持20+种数据源和目标
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
- [x] **SqliteReader**: 从SQLite数据库读取数据，支持ROWID优化
- [x] **SqlServerReader**: 从SQL Server数据库读取数据
- [x] **OracleReader**: 从Oracle数据库读取数据 **(新增，基于go-ora纯Go驱动)**
- [x] **ClickHouseReader**: 从ClickHouse列存数据库读取数据
- [x] **DorisReader**: 从Apache Doris数据库读取数据

#### Writer插件
- [x] **PostgresqlWriter**: 向PostgreSQL数据库写入数据，支持批量写入
- [x] **MysqlWriter**: 向MySQL数据库写入数据，支持事务批处理
- [x] **SqliteWriter**: 向SQLite数据库写入数据
- [x] **SqlServerWriter**: 向SQL Server数据库写入数据
- [x] **OracleWriter**: 向Oracle数据库写入数据 **(新增，基于go-ora纯Go驱动)**
- [x] **ClickHouseWriter**: 向ClickHouse列存数据库写入数据
- [x] **DorisWriter**: 向Apache Doris数据库写入数据

### 📄 文件插件

#### Reader插件
- [x] **TxtFileReader**: 读取文本文件，支持CSV、TSV等格式
- [x] **JsonFileReader**: 读取JSON/JSONL文件 **(新增)**
  - 🔍 **智能模式检测**: 自动识别JSON vs JSONL格式
  - 📋 **模式推断**: 预读100行自动推断字段类型
  - 🌐 **Glob支持**: 支持`/*/*.jsonl`等通配符模式
  - 🗂️ **递归遍历**: 支持目录递归扫描

#### Writer插件
- [x] **TxtFileWriter**: 写入文本文件，支持多种分隔符
- [x] **JsonFileWriter**: 写入JSON/JSONL文件 **(新增)**
  - 📝 **JSONL优化**: 默认JSONL格式避免内存问题
  - ✂️ **智能分片**: 支持按文件大小和记录数自动分片
  - 🎯 **类型保持**: 保持原始数据类型（数字、布尔、日期）

### 🏪 NoSQL插件

#### Reader/Writer插件
- [x] **MongoReader/MongoWriter**: MongoDB文档数据库支持
- [x] **StreamReader/StreamWriter**: 流式数据处理

## 🚀 新功能亮点

### Oracle数据库支持（基于go-ora）
- ✅ **纯Go实现**: 无需安装Oracle客户端库
- ✅ **简化部署**: 单二进制包含所有依赖
- ✅ **全类型支持**: NUMBER、VARCHAR2、DATE、CLOB、BLOB等
- ✅ **高性能**: 批量插入、事务处理、连接池

### JSON文件处理
- ✅ **智能推断**: 自动检测字段类型（long、double、boolean、date、string）
- ✅ **格式兼容**: 同时支持标准JSON和行分隔JSON（JSONL）
- ✅ **模式灵活**: 支持嵌套对象、数组、复杂类型
- ✅ **性能优化**: 流式处理大文件，内存占用可控

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
- **ClickHouse**: `github.com/ClickHouse/clickhouse-go/v2` - 官方驱动
- **MongoDB**: `go.mongodb.org/mongo-driver` - 官方驱动

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

### v1.3.0 (最新) 🆕
- **新增**: Oracle数据库支持（基于go-ora纯Go驱动）
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
- **支持**: PostgreSQL、MySQL、SQLite数据库
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