# ClickHouseWriter插件文档

## 1. 快速介绍

ClickHouseWriter插件实现了向ClickHouse分析型数据库写入数据的功能。在底层实现上，ClickHouseWriter通过ClickHouse原生驱动连接远程ClickHouse数据库，并根据用户配置的信息，将DataX传输协议的数据写入ClickHouse数据库。

ClickHouse作为列式存储的分析型数据库，在批量数据写入方面有着出色的性能表现。

## 2. 实现原理

ClickHouseWriter通过ClickHouse原生Go驱动连接远程ClickHouse数据库，获取DataX传输的数据，并根据您配置的`writeMode`、`batchSize`等信息，将数据写入ClickHouse，当出现写入失败情况时，进行数据重试。

ClickHouseWriter面向ETL开发工程师，他们使用ClickHouseWriter从数仓导入数据到ClickHouse。同时ClickHouseWriter亦可以作为数据迁移工具为DBA等用户提供服务。

## 3. 功能说明

### 3.1 配置样例

以下是一个向ClickHouse数据库写入数据的配置范例：

```json
{
    "name": "clickhousewriter",
    "parameter": {
        "username": "default",
        "password": "",
        "connection": [
            {
                "jdbcUrl": "clickhouse://127.0.0.1:9000/test_db",
                "table": ["test_table"]
            }
        ],
        "column": ["id", "name", "age", "email", "created_at"],
        "batchSize": 65536,
        "batchByteSize": 134217728,
        "writeMode": "insert",
        "preSql": ["TRUNCATE TABLE test_table"],
        "postSql": ["OPTIMIZE TABLE test_table"]
    }
}
```

### 3.2 参数说明

#### jdbcUrl
- 描述：ClickHouse数据库的连接信息，使用ClickHouse原生协议
- 必选：是
- 格式：String，格式为clickhouse://host:port/database
- 默认值：无

#### username
- 描述：ClickHouse数据库的用户名
- 必选：是
- 默认值：无

#### password
- 描述：ClickHouse数据库的密码
- 必选：是（如果ClickHouse数据库未配置密码则可为空）
- 默认值：无

#### table
- 描述：需要写入的表名
- 必选：是
- 格式：List<String>，通常只配置一个表
- 默认值：无

#### column
- 描述：需要写入的字段列表
- 必选：是
- 格式：List<String>
- 默认值：无

#### batchSize
- 描述：每批次最大行数
- 必选：否
- 格式：Int
- 默认值：65536

#### batchByteSize
- 描述：每批次最大字节数，单位为字节
- 必选：否
- 格式：Long
- 默认值：134217728（128MB）

#### writeMode
- 描述：写入模式
- 必选：否
- 格式：String
- 默认值："insert"
- 可选值：insert

#### preSql
- 描述：写入数据到目的表前，会先执行这里的标准语句
- 必选：否
- 格式：List<String>
- 默认值：无

#### postSql
- 描述：写入数据到目的表后，会执行这里的标准语句
- 必选：否
- 格式：List<String>
- 默认值：无

#### session
- 描述：ClickHouse会话级参数配置
- 必选：否
- 格式：Map<String, String>
- 默认值：无

### 3.3 类型转换

ClickHouseWriter针对ClickHouse数据类型转换列表：

| DataX内部类型 | ClickHouse数据类型                     |
|------------|-------------------------------------|
| Long       | UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64 |
| Double     | Float32, Float64, Decimal           |
| String     | String, FixedString, UUID, IPv4, IPv6 |
| Date       | Date, Date32, DateTime, DateTime64  |
| Boolean    | Boolean                             |
| Bytes      | Array(UInt8)                        |
| Array      | Array types (从JSON字符串转换)         |

### 3.4 写入模式

ClickHouseWriter目前支持的写入模式：

#### insert模式
直接向表中插入数据，这是默认的写入模式。

### 3.5 批量写入

ClickHouseWriter采用批量写入模式，具有以下特点：

#### 批量策略
- 按行数控制：达到batchSize行数时触发写入
- 按字节数控制：达到batchByteSize字节数时触发写入
- 按时间控制：定时刷新缓冲区

#### 性能优化
ClickHouseWriter使用ClickHouse的批量插入API，能够达到很高的写入吞吐量。

## 4. 性能报告

### 4.1 环境准备

#### 数据特征
建表语句：
```sql
CREATE TABLE test_table (
    id UInt64,
    name String,
    age UInt32,
    email String,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY id;
```

单行记录大小约为100字节。

#### 机器参数
- 执行DataX的机器参数为：1台8核64G
- ClickHouse数据库配置：单节点，8核32G

#### DataX JVM参数
-Xms8192m -Xmx8192m -XX:+UseG1GC

### 4.2 测试报告

| 通道数 | 批次大小 | DataX速度(Rec/s) | DataX流量(MB/s) | 说明 |
|-------|---------|------------------|-----------------|------|
| 1     | 65536   | 200,000         | 20.0           | 单通道 |
| 4     | 65536   | 750,000         | 75.0           | 多通道 |
| 8     | 65536   | 1,200,000       | 120.0          | 高并发 |

## 5. 约束限制

### 5.1 网络连接
ClickHouseWriter需要能够访问ClickHouse数据库的TCP端口（默认9000）。

### 5.2 数据类型
- 目前支持ClickHouse的基本数据类型
- Array类型支持从JSON字符串转换
- 不支持Tuple、Nested等复杂嵌套类型

### 5.3 写入限制
- 只支持insert写入模式
- 不支持UPDATE、DELETE等操作
- 建议使用MergeTree引擎族的表

### 5.4 事务限制
- ClickHouse对事务支持有限
- 每个批次的写入是原子的，但整个任务不保证事务一致性

## 6. FAQ

**Q: ClickHouseWriter如何保证数据不重复？**

A: ClickHouseWriter本身不提供去重机制，建议在ClickHouse层面通过ReplacingMergeTree等引擎或者业务逻辑来保证数据唯一性。

**Q: ClickHouseWriter支持哪些数据格式？**

A: ClickHouseWriter使用ClickHouse的原生二进制协议，性能优于HTTP接口。

**Q: ClickHouseWriter的性能如何优化？**

A: 可以通过以下方式优化性能：
1. 增加通道数（channel）
2. 调整批次大小（batchSize）
3. 使用合适的ClickHouse表引擎
4. 合理设计表的排序键

**Q: 写入失败怎么办？**

A: ClickHouseWriter会自动重试失败的批次，如果重试仍然失败，任务会终止并报告错误信息。建议检查网络连接、表结构和数据格式。