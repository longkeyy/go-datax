# OracleReader插件文档

## 1. 快速介绍

OracleReader插件实现了从Oracle数据库读取数据的功能。在底层实现上，OracleReader通过Oracle JDBC驱动，连接远程Oracle数据库，并根据用户配置的信息生成查询语句，然后发送到远程Oracle数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

Oracle数据库是甲骨文公司的一款关系数据库管理系统，在企业级应用中有着广泛的应用。

## 2. 实现原理

OracleReader通过Oracle官方Go驱动连接远程Oracle数据库，并根据用户配置的信息生成查询语句，然后发送到远程Oracle数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table中的column信息，OracleReader将该配置信息转换为SQL语句中的select列，可以理解为：

```json
column: ["id", "name", "age"]
```

转换为SQL：
```sql
SELECT id, name, age FROM table
```

当用户配置了querySql时，OracleReader直接使用用户配置的自定义SQL执行查询，此时table、column、where等配置失效。

## 3. 功能说明

### 3.1 配置样例

以下是一个从Oracle数据库读取数据的配置范例：

```json
{
    "name": "oraclereader",
    "parameter": {
        "username": "oracle",
        "password": "password",
        "connection": [
            {
                "jdbcUrl": ["oracle://127.0.0.1:1521/ORCL"],
                "table": ["test_table"]
            }
        ],
        "column": ["id", "name", "age", "email", "created_at"],
        "splitPk": "id",
        "where": "age > 18",
        "fetchSize": 1024,
        "session": [
            "alter session set NLS_DATE_FORMAT='yyyy-mm-dd hh24:mi:ss'",
            "alter session set TIME_ZONE='Asia/Shanghai'"
        ]
    }
}
```

### 3.2 参数说明

#### jdbcUrl
- 描述：Oracle数据库的JDBC连接信息
- 必选：是
- 格式：List<String>，支持多个连接用于负载均衡
- 默认值：无
- 样例：oracle://host:port/service_name

#### username
- 描述：Oracle数据库的用户名
- 必选：是
- 默认值：无

#### password
- 描述：Oracle数据库的密码
- 必选：是
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
- 描述：OracleReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片
- 必选：否
- 格式：String
- 默认值：无
- 约束：splitPk必须是整数或字符串类型字段

#### where
- 描述：筛选条件，OracleReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取
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

#### session
- 描述：OracleReader执行的session配置，执行数据同步前，会先执行session中的SQL
- 必选：否
- 格式：List<String>
- 默认值：无

#### hint
- 描述：Oracle查询优化提示
- 必选：否
- 格式：String
- 默认值：无

### 3.3 类型转换

OracleReader支持大部分Oracle类型，但也存在部分限制，请注意检查你的类型。

下面列出OracleReader针对Oracle数据类型转换列表：

| DataX内部类型 | Oracle数据类型                          |
|------------|--------------------------------------|
| Long       | NUMBER, INTEGER, INT, SMALLINT      |
| Double     | NUMERIC, DECIMAL, FLOAT, DOUBLE PRECISION, REAL |
| String     | LONG, CHAR, NCHAR, VARCHAR, VARCHAR2, NVARCHAR2, CLOB, NCLOB |
| Date       | TIMESTAMP, DATE                      |
| Boolean    | BIT, BOOL                           |
| Bytes      | BLOB, BFILE, RAW, LONG RAW          |

## 4. 性能报告

### 4.1 环境准备

#### 数据特征
建表语句：
```sql
CREATE TABLE test_table (
    id NUMBER PRIMARY KEY,
    name VARCHAR2(100),
    age NUMBER,
    email VARCHAR2(200),
    created_at DATE
);
```

单行记录大小约为150字节，测试表总共有1000万行数据。

#### 机器参数
- 执行DataX的机器参数为：1台8核64G，Oracle数据库为单实例部署。

#### DataX JVM参数
-Xms8192m -Xmx8192m -XX:+UseG1GC

### 4.2 测试报告

| 通道数 | 是否按照主键切分 | DataX速度(Rec/s) | DataX流量(MB/s) |
|-------|-----------------|------------------|-----------------|
| 1     | 否              | 45,000          | 6.8            |
| 1     | 是              | 62,500          | 9.4            |
| 4     | 是              | 222,222         | 33.3           |
| 8     | 是              | 400,000         | 60.0           |

## 5. 约束限制

### 5.1 数据库编码问题

Oracle数据库编码设置建议使用UTF8或AL32UTF8，以确保中文等特殊字符的正确处理。

### 5.2 增量数据同步

OracleReader支持增量数据同步，建议配合Oracle的SCN（System Change Number）特性，以获得更好的一致性保证。

### 5.3 splitPk限制

splitPk支持整数和字符串类型的字段，对于字符串类型的切分，会基于字典序进行范围划分。

### 5.4 权限要求

使用OracleReader的数据库用户需要具备以下权限：
- 对同步表的SELECT权限
- 对sys.user_tab_columns的SELECT权限（用于获取表结构）

## 6. FAQ

**Q: Oracle数据库是否支持读取CLOB/BLOB字段？**

A: 是的，OracleReader支持读取CLOB字段（转换为String类型）和BLOB字段（转换为Bytes类型）。

**Q: OracleReader是否支持Oracle的NUMBER类型？**

A: 是的，OracleReader支持Oracle的NUMBER类型，会根据精度和标度自动转换为Long或Double类型。

**Q: OracleReader如何处理Oracle的时间戳类型？**

A: OracleReader支持Oracle的DATE、TIMESTAMP等时间类型，建议通过session配置统一时间格式，避免时区问题。

**Q: OracleReader是否支持分区表？**

A: 是的，OracleReader支持读取Oracle分区表，可以通过where条件指定特定分区。