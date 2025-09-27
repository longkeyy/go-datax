# DorisReader插件文档

## 1. 快速介绍

DorisReader插件实现了从Apache Doris分析型数据库读取数据的功能。在底层实现上，DorisReader通过MySQL JDBC驱动，连接远程Doris数据库，并根据用户配置的信息生成查询语句，然后发送到远程Doris数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

Doris是一个现代化的MPP（大规模并行处理）分析型数据库，兼容MySQL协议，因此DorisReader可以像操作MySQL一样操作Doris数据库。

## 2. 实现原理

DorisReader通过MySQL兼容的JDBC驱动连接远程Doris数据库，并根据用户配置的信息生成查询语句，然后发送到远程Doris数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table中的column信息，DorisReader将该配置信息转换为SQL语句中的select列，可以理解为：

```json
column: ["id", "name", "age"]
```

转换为SQL：
```sql
SELECT id, name, age FROM table
```

当用户配置了querySql时，DorisReader直接使用用户配置的自定义SQL执行查询，此时table、column、where等配置失效。

## 3. 功能说明

### 3.1 配置样例

以下是一个从Doris数据库读取数据的配置范例：

```json
{
    "name": "dorisreader",
    "parameter": {
        "username": "root",
        "password": "password",
        "connection": [
            {
                "jdbcUrl": ["jdbc:mysql://127.0.0.1:9030/test_db"],
                "table": ["test_table"]
            }
        ],
        "column": ["id", "name", "age", "email", "created_at"],
        "splitPk": "id",
        "where": "age > 18"
    }
}
```

### 3.2 参数说明

#### jdbcUrl
- 描述：Doris数据库的JDBC连接信息，使用MySQL协议
- 必选：是
- 格式：List<String>，支持多个连接用于负载均衡
- 默认值：无
- 样例：jdbc:mysql://ip:port/database

#### username
- 描述：Doris数据库的用户名
- 必选：是
- 默认值：无

#### password
- 描述：Doris数据库的密码
- 必选：是（如果Doris数据库未配置密码则忽略）
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
- 描述：DorisReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片
- 必选：否
- 格式：String
- 默认值：无
- 约束：splitPk必须是整数类型字段

#### where
- 描述：筛选条件，DorisReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取
- 必选：否
- 格式：String
- 默认值：无

#### querySql
- 描述：在有些业务场景下，where这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL
- 必选：否
- 格式：String
- 默认值：无

### 3.3 类型转换

DorisReader支持大部分Doris类型，但也存在部分限制，请注意检查你的类型。

下面列出DorisReader针对Doris数据类型转换列表：

| DataX内部类型 | Doris数据类型                           |
|------------|---------------------------------------|
| Long       | int, tinyint, smallint, int, bigint, largint |
| Double     | float, double, decimal                |
| String     | varchar, char, text, string, map, json, array, struct |
| Date       | date, datetime                        |
| Boolean    | boolean                               |

## 4. 性能报告

### 4.1 环境准备

#### 数据特征
建表语句：
```sql
CREATE TABLE test_table (
    id BIGINT NOT NULL,
    name VARCHAR(32) NOT NULL,
    age INT,
    email VARCHAR(64),
    created_at DATETIME
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 8;
```

单行记录大小约为100字节，测试表总共有1000万行数据。

#### 机器参数
- 执行DataX的机器参数为：1台8核64G，Doris数据库为3FE + 3BE的分布式集群。

#### DataX JVM参数
-Xms8192m -Xmx8192m -XX:+UseG1GC

### 4.2 测试报告

| 通道数 | 是否按照主键切分 | DataX速度(Rec/s) | DataX流量(MB/s) |
|-------|-----------------|------------------|-----------------|
| 1     | 否              | 52,235          | 5.25           |
| 1     | 是              | 79,034          | 7.94           |
| 4     | 是              | 267,129         | 26.84          |
| 8     | 是              | 431,915         | 43.40          |

## 5. 约束限制

### 5.1 数据库编码问题

Doris本身的编码设置非常灵活，建议设置为UTF-8。

### 5.2 增量数据同步

DorisReader支持增量数据同步，建议在业务低峰期进行，以免影响在线业务。

### 5.3 splitPk限制

splitPk仅支持整数类型的字段，不支持浮点、字符串、日期等类型。

## 6. FAQ

**Q: Doris数据库是否支持同步读取？**

A: Doris数据库支持同步读取，但建议在业务低峰期进行，以降低对在线业务的影响。

**Q: DorisReader是否支持读取Doris的复杂数据类型？**

A: DorisReader支持读取Doris的Map、Array、JSON等复杂类型，但会将其转换为字符串类型进行传输。

**Q: DorisReader如何处理NULL值？**

A: DorisReader会将Doris中的NULL值转换为DataX的NULL表示，下游Writer需要根据自身特点进行处理。