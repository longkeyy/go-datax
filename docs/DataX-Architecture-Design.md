# DataX 核心架构设计文档

## 1. 总体架构概述

DataX采用框架+插件的架构模式，将数据同步抽象为Reader插件、Framework、Writer插件三个核心组件。

```
+-------------------+
|   DataX Engine    |
+-------------------+
|                   |
| +---------------+ |
| | JobContainer  | |
| +---------------+ |
|        |          |
| +---------------+ |
| |TaskGroupCont- | |
| |   ainer       | |
| +---------------+ |
|        |          |
| +-----+  +------+ |
| |Read-|  |Write-| |
| | er  |  | r    | |
| +-----+  +------+ |
+-------------------+
```

## 2. 核心组件设计

### 2.1 Engine（引擎）

**职责**: 程序入口，负责命令行参数解析、配置加载、容器启动

**关键特性**:
- 命令行参数处理（-job、-jobid、-mode）
- 配置文件解析与验证
- 容器类型判断（Job vs TaskGroup）
- 全局错误处理和退出码管理

**核心方法**:
```java
public static void entry(String[] args)
public void start(Configuration allConf)
```

### 2.2 JobContainer（作业容器）

**职责**: Job级别的生命周期管理，负责插件初始化、任务拆分、调度执行

**生命周期阶段**:
1. **init()** - 插件加载和配置验证
2. **prepare()** - 预处理阶段（Writer准备）
3. **split()** - 任务拆分
4. **schedule()** - 任务调度执行
5. **post()** - 后处理
6. **destroy()** - 资源销毁

**任务拆分策略**:
- Reader按照adviceNumber进行拆分（建议值）
- Writer按照mandatoryNumber进行拆分（强制值，必须与Reader任务数相等）
- 支持splitPk字段进行数据范围拆分

**关键配置**:
```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1,     // 并发通道数
        "byte": 1048576   // 速度限制
      }
    }
  }
}
```

### 2.3 TaskGroupContainer（任务组容器）

**职责**: 管理并发执行的Reader-Writer任务对

**执行模型**:
- 每个TaskGroup包含一个Reader Task和一个Writer Task
- 通过Channel进行Reader和Writer之间的数据传输
- 支持并发执行多个TaskGroup

**数据流**:
```
Reader Task -> Channel -> Writer Task
     |                        |
RecordSender          RecordReceiver
```

### 2.4 插件系统设计

#### 2.4.1 Reader插件架构

**接口层次**:
```java
public abstract class Reader {
    public static abstract class Job extends AbstractJobPlugin {
        public abstract List<Configuration> split(int adviceNumber);
    }

    public static abstract class Task extends AbstractTaskPlugin {
        public abstract void startRead(RecordSender recordSender);
    }
}
```

**PostgreSQL Reader特性**:
- 支持JDBC连接池
- 支持splitPk字段进行数据分片
- 支持自定义SQL查询（querySql）
- 支持WHERE条件过滤
- 支持列裁剪和列换序

#### 2.4.2 Writer插件架构

**接口层次**:
```java
public abstract class Writer {
    public static abstract class Job extends AbstractJobPlugin {
        public abstract List<Configuration> split(int mandatoryNumber);
    }

    public static abstract class Task extends AbstractTaskPlugin {
        public abstract void startWrite(RecordReceiver recordReceiver);
    }
}
```

**PostgreSQL Writer特性**:
- 支持批量写入优化
- 支持preSql和postSql执行
- 支持事务管理
- 支持错误重试机制

### 2.5 数据模型设计

#### 2.5.1 Record接口
```java
public interface Record {
    void addColumn(Column column);
    void setColumn(int i, Column column);
    Column getColumn(int i);
    int getColumnNumber();
    int getByteSize();
}
```

#### 2.5.2 Column类型系统
```java
public abstract class Column {
    // 支持的数据类型
    public enum Type {
        NULL, LONG, DOUBLE, STRING, DATE, BOOL, BYTES
    }

    // 类型转换方法
    public abstract String asString();
    public abstract Long asLong();
    public abstract Double asDouble();
    public abstract Date asDate();
    public abstract Boolean asBoolean();
    public abstract byte[] asBytes();
}
```

**类型映射关系**:

| PostgreSQL类型 | DataX内部类型 | 说明 |
|----------------|---------------|------|
| bigint, serial, integer, smallint | Long | 整数类型 |
| double precision, numeric, real, money | Double | 浮点数类型 |
| varchar, char, text, bit, inet | String | 字符串类型 |
| date, time, timestamp | Date | 日期时间类型 |
| boolean | Boolean | 布尔类型 |
| bytea | Bytes | 二进制类型 |

### 2.6 配置管理系统

#### 2.6.1 Configuration类设计
```java
public class Configuration {
    // 核心方法
    public void set(String path, Object value);
    public Object get(String path);
    public String getString(String path);
    public List<Configuration> getListConfiguration(String path);
    public Configuration clone();
}
```

#### 2.6.2 配置文件格式
```json
{
  "job": {
    "setting": {
      "speed": {"channel": 1},
      "errorLimit": {"record": 0, "percentage": 0.02}
    },
    "content": [{
      "reader": {
        "name": "postgresqlreader",
        "parameter": {
          "username": "postgres",
          "password": "postgres",
          "connection": [{
            "jdbcUrl": ["jdbc:postgresql://localhost:5432/source_db"],
            "table": ["table_name"]
          }],
          "column": ["*"],
          "splitPk": "id",
          "where": "id > 100",
          "fetchSize": 1024
        }
      },
      "writer": {
        "name": "postgresqlwriter",
        "parameter": {
          "username": "postgres",
          "password": "postgres",
          "connection": [{
            "jdbcUrl": "jdbc:postgresql://localhost:5432/target_db",
            "table": ["table_name"]
          }],
          "column": ["*"],
          "preSql": ["DELETE FROM table_name"],
          "postSql": ["ANALYZE table_name"],
          "batchSize": 1024
        }
      }
    }]
  }
}
```

## 3. 关键设计模式

### 3.1 Template Method模式
AbstractJobPlugin和AbstractTaskPlugin使用模板方法模式，定义插件生命周期：
- init() -> prepare() -> startRead()/startWrite() -> post() -> destroy()

### 3.2 Factory Method模式
插件通过反射和配置进行动态加载：
```java
LoadUtil.loadJobPlugin(PluginType.READER, readerPluginName)
```

### 3.3 Producer-Consumer模式
Reader和Writer通过Channel进行解耦：
- Reader作为Producer生产Record
- Writer作为Consumer消费Record
- Channel提供缓冲和流控

### 3.4 Strategy模式
不同的数据源通过不同的Reader/Writer插件实现策略模式

## 4. 性能优化设计

### 4.1 并发控制
- 支持多Channel并发传输
- Reader支持splitPk进行数据分片
- Writer支持批量提交减少网络开销

### 4.2 内存管理
- Record采用流式处理，避免全量加载
- Channel缓冲区大小可配置
- 支持大数据量的流式传输

### 4.3 容错机制
- 支持脏数据记录和统计
- 支持错误记录百分比和绝对数量限制
- 支持任务级别的错误重试

## 5. go-datax实现对比分析

### 5.1 已实现的核心组件

| Java组件 | Go实现 | 完成度 | 说明 |
|---------|--------|--------|------|
| Engine | core/engine/engine.go | ✅ | 基本功能完成 |
| JobContainer | core/job/jobcontainer.go | ✅ | 生命周期管理完成 |
| TaskGroupContainer | core/taskgroup/taskgroupcontainer.go | ✅ | 并发执行完成 |
| Configuration | common/config/configuration.go | ✅ | 配置管理完成 |
| Record/Column | common/element/ | ✅ | 数据模型完成 |
| Plugin Registry | common/plugin/registry.go | ✅ | 插件注册完成 |
| PostgreSQL Reader | plugins/reader/postgresqlreader/ | ✅ | 基本功能完成 |
| PostgreSQL Writer | plugins/writer/postgresqlwriter/ | ✅ | 基本功能完成 |

### 5.2 需要完善的功能

1. **错误处理机制**: 需要实现更完善的错误统计和限制
2. **性能监控**: 需要添加统计信息收集
3. **splitPk优化**: 需要优化分片算法
4. **事务管理**: Writer需要添加事务支持
5. **连接池管理**: 需要优化数据库连接管理
6. **配置验证**: 需要添加更严格的配置验证

### 5.3 Go语言特性利用

1. **Goroutine**: 利用轻量级协程替代Java线程
2. **Channel**: 使用Go原生Channel替代Java的BlockingQueue
3. **Interface**: 使用Go接口实现插件抽象
4. **Struct Embedding**: 利用Go的组合特性简化继承关系

## 6. 下一步实现计划

1. 验证基本数据同步功能
2. 完善错误处理和统计机制
3. 优化性能和内存使用
4. 添加更多插件支持
5. 完善测试覆盖率