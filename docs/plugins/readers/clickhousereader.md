# ClickHouseReader插件文档

## 1. 快速介绍

ClickHouseReader插件实现了从ClickHouse分析型数据库读取数据的功能。在底层实现上，ClickHouseReader通过ClickHouse原生驱动连接远程ClickHouse数据库，并根据用户配置的信息生成查询语句，然后发送到远程ClickHouse数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

ClickHouse是一个用于联机分析处理(OLAP)的列式数据库管理系统(DBMS)，特别适合大数据量的分析查询。

## 2. 实现原理

ClickHouseReader通过ClickHouse原生Go驱动连接远程ClickHouse数据库，并根据用户配置的信息生成查询语句，然后发送到远程ClickHouse数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table中的column信息，ClickHouseReader将该配置信息转换为SQL语句中的select列，可以理解为：

```json
column: ["id", "name", "age"]
```

转换为SQL：
```sql
SELECT id, name, age FROM table
```

当用户配置了querySql时，ClickHouseReader直接使用用户配置的自定义SQL执行查询，此时table、column、where等配置失效。

## 3. 功能说明

### 3.1 配置样例

以下是一个从ClickHouse数据库读取数据的配置范例：

```json
{
    "name": "clickhousereader",
    "parameter": {
        "username": "default",
        "password": "",
        "connection": [
            {
                "jdbcUrl": ["clickhouse://127.0.0.1:9000/test_db"],
                "table": ["test_table"]
            }
        ],
        "column": ["id", "name", "age", "email", "created_at"],
        "splitPk": "id",
        "where": "age > 18",
        "fetchSize": 1024
    }
}
```

### 3.2 参数说明

#### jdbcUrl
- 描述：ClickHouse数据库的连接信息，使用ClickHouse原生协议
- 必选：是
- 格式：List<String>，支持多个连接用于负载均衡
- 默认值：无
- 样例：clickhouse://host:port/database

#### username
- 描述：ClickHouse数据库的用户名
- 必选：是
- 默认值：无

#### password
- 描述：ClickHouse数据库的密码
- 必选：是（如果ClickHouse数据库未配置密码则可为空）
- 默认值：无

#### table
- 描述：所选取的需要同步的表名，支持多表
- 必选：是
- 格式：List<String>
- 默认值：无

#### column
- 描述：所配置的表中需要同步的列名集合
- 必选：是
- 格式：List<String>
- 默认值：无
- 特殊值：["*"] 表示同步所有列

#### splitPk
- 描述：ClickHouseReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片
- 必选：否
- 格式：String
- 默认值：无
- 约束：splitPk必须是整数类型字段

#### where
- 描述：筛选条件，ClickHouseReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取
- 必选：否
- 格式：String
- 默认值：无

#### querySql
- 描述：在有些业务场景下，where这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL
- 必选：否
- 格式：String
- 默认值：无

#### fetchSize
- 描述：该配置项定义了插件和数据库服务器端每次批量数据获取条数
- 必选：否
- 格式：Int
- 默认值：1024
- 建议：不建议设置过大，可能导致内存溢出

### 3.3 类型转换

ClickHouseReader支持大部分ClickHouse类型，但也存在部分限制，请注意检查你的类型。

下面列出ClickHouseReader针对ClickHouse数据类型转换列表：

| DataX内部类型 | ClickHouse数据类型                     |
|------------|-------------------------------------|
| Long       | UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64 |
| Double     | Float32, Float64, Decimal           |
| String     | String, FixedString, UUID, IPv4, IPv6 |
| Date       | Date, Date32, DateTime, DateTime64  |
| Boolean    | Boolean                             |
| Bytes      | Array(UInt8)                        |
| Array      | Array types (转换为JSON字符串)          |

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

单行记录大小约为100字节，测试表总共有1000万行数据。

#### 机器参数
- 执行DataX的机器参数为：1台8核64G，ClickHouse数据库为单节点部署。

#### DataX JVM参数
-Xms8192m -Xmx8192m -XX:+UseG1GC

### 4.2 测试报告

| 通道数 | 是否按照主键切分 | DataX速度(Rec/s) | DataX流量(MB/s) |
|-------|-----------------|------------------|-----------------|
| 1     | 否              | 125,000         | 12.5           |
| 1     | 是              | 142,857         | 14.3           |
| 4     | 是              | 500,000         | 50.0           |
| 8     | 是              | 800,000         | 80.0           |

## 5. 约束限制

### 5.1 数据库编码问题

ClickHouse本身的编码设置非常灵活，建议设置为UTF-8。

### 5.2 增量数据同步

ClickHouseReader支持增量数据同步，建议配合ClickHouse的分区表特性，以获得更好的查询性能。

### 5.3 splitPk限制

splitPk仅支持整数类型的字段，不支持浮点、字符串、日期等类型。

### 5.4 数据一致性

由于ClickHouse是面向分析的数据库，多线程并发读取时无法保证完美的一致性快照，这是ClickHouse本身的特性。

## 6. FAQ

**Q: ClickHouse数据库是否支持事务？**

A: ClickHouse对事务的支持有限，主要面向OLAP场景，建议在业务低峰期进行数据同步。

**Q: ClickHouseReader是否支持读取ClickHouse的复杂数据类型？**

A: ClickHouseReader支持读取Array类型，会将其转换为JSON字符串；但不支持Tuple、Nested等复杂嵌套类型。

**Q: ClickHouseReader如何处理NULL值？**

A: ClickHouseReader会将ClickHouse中的NULL值转换为DataX的NULL表示，下游Writer需要根据自身特点进行处理。