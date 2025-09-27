# go-datax

golang版本的DataX，完全兼容DataX配置文件格式的数据同步工具。

## 项目架构

### 核心模块
- **core**: 任务执行引擎、容器管理
- **common**: 通用组件、接口定义、数据类型
- **plugins**: 插件实现
  - **reader**: 数据读取插件
  - **writer**: 数据写入插件

### 设计原理

1. **引擎(Engine)**: 程序入口，负责解析命令行参数、初始化配置
2. **作业容器(JobContainer)**: 负责作业的生命周期管理、任务拆分、调度
3. **任务组容器(TaskGroupContainer)**: 管理并发任务组的执行
4. **插件系统**: Reader和Writer插件，支持多种数据源

### 数据流程

1. 读取配置文件，解析作业参数
2. 初始化Reader和Writer插件
3. 拆分任务，创建多个并发通道
4. 执行数据传输：Reader -> Channel -> Writer
5. 监控进度，收集统计信息

## 支持的插件

### Reader插件
- [x] PostgresqlReader: 从PostgreSQL数据库读取数据

### Writer插件
- [x] PostgresqlWriter: 向PostgreSQL数据库写入数据

## 配置文件格式

兼容DataX原生JSON配置格式：

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1
      }
    },
    "content": [
      {
        "reader": {
          "name": "postgresqlreader",
          "parameter": {
            "username": "postgres",
            "password": "postgres",
            "jdbcUrl": ["jdbc:postgresql://localhost:5432/llm_datasets"],
            "table": ["table_name"],
            "column": ["*"]
          }
        },
        "writer": {
          "name": "postgresqlwriter",
          "parameter": {
            "username": "postgres",
            "password": "postgres",
            "jdbcUrl": "jdbc:postgresql://localhost:5432/datax",
            "table": ["table_name"],
            "column": ["*"]
          }
        }
      }
    ]
  }
}
```

## 使用方法

```bash
go run cmd/datax/main.go -job config.json
```

## 技术栈

- Go 1.22+
- GORM (PostgreSQL连接池管理)
- pq (PostgreSQL驱动)