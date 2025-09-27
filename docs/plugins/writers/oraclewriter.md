# OracleWriter插件文档

## 1. 快速介绍

OracleWriter插件实现了向Oracle数据库写入数据的功能。在底层实现上，OracleWriter通过Oracle官方Go驱动，连接远程Oracle数据库，并根据用户配置的信息生成批量写入语句，然后发送到远程Oracle数据库执行，从而实现向Oracle数据库写入数据的功能。

Oracle数据库是甲骨文公司的一款关系数据库管理系统，在企业级应用中有着广泛的应用。

## 2. 实现原理

OracleWriter通过Oracle官方Go驱动连接远程Oracle数据库，并根据用户配置的信息生成写入语句，然后发送到远程Oracle数据库执行。

根据你配置的writeMode生成语句。

* 当writeMode配置为INSERT时，OracleWriter将使用INSERT INTO语句插入数据到目标表，遇到重复主键时任务失败。
* 当writeMode配置为MERGE时，OracleWriter将使用MERGE语句，相当于执行REPLACE INTO，新数据将会覆盖旧数据。

## 3. 功能说明

### 3.1 配置样例

以下是一个向Oracle数据库写入数据的配置范例：

```json
{
    "name": "oraclewriter",
    "parameter": {
        "username": "oracle",
        "password": "password",
        "writeMode": "INSERT",
        "connection": [
            {
                "jdbcUrl": "oracle://127.0.0.1:1521/ORCL",
                "table": ["test_table"]
            }
        ],
        "column": ["id", "name", "age", "email", "created_at"],
        "batchSize": 1024,
        "preSql": [
            "alter session set NLS_DATE_FORMAT='yyyy-mm-dd hh24:mi:ss'",
            "alter session set TIME_ZONE='Asia/Shanghai'"
        ],
        "postSql": ["COMMIT"]
    }
}
```

### 3.2 参数说明

#### jdbcUrl
- 描述：Oracle数据库的JDBC连接信息
- 必选：是
- 格式：oracle://host:port/service_name
- 默认值：无

#### username
- 描述：Oracle数据库的用户名
- 必选：是
- 默认值：无

#### password
- 描述：Oracle数据库的密码
- 必选：是
- 默认值：无

#### table
- 描述：所选取的需要写入的表名，支持多表
- 必选：是
- 格式：List<String>
- 默认值：无

#### column
- 描述：所配置的表中需要写入的列名集合
- 必选：是
- 格式：List<String>
- 默认值：无
- 特殊值：["*"] 表示写入所有列

#### writeMode
- 描述：写入模式，支持INSERT和MERGE两种模式
- 必选：否
- 格式：String
- 默认值：INSERT
- 可选值：INSERT, MERGE

#### batchSize
- 描述：每次批量写入的记录数
- 必选：否
- 格式：Int
- 默认值：1024
- 建议：不建议设置过大，可能导致内存溢出

#### preSql
- 描述：写入数据前执行的SQL语句
- 必选：否
- 格式：List<String>
- 默认值：无

#### postSql
- 描述：写入数据后执行的SQL语句
- 必选：否
- 格式：List<String>
- 默认值：无

#### session
- 描述：OracleWriter执行的session配置，数据同步前会先执行session中的SQL
- 必选：否
- 格式：List<String>
- 默认值：无

### 3.3 类型转换

OracleWriter支持大部分Oracle类型，但也存在部分限制，请注意检查你的类型。

下面列出OracleWriter针对Oracle数据类型转换列表：

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

单行记录大小约为150字节，测试表总共写入1000万行数据。

#### 机器参数
- 执行DataX的机器参数为：1台8核64G，Oracle数据库为单实例部署。

#### DataX JVM参数
-Xms8192m -Xmx8192m -XX:+UseG1GC

### 4.2 测试报告

| 通道数 | 批量大小 | DataX速度(Rec/s) | DataX流量(MB/s) |
|-------|---------|------------------|-----------------|
| 1     | 1024    | 40,000          | 6.0            |
| 1     | 2048    | 55,555          | 8.3            |
| 4     | 1024    | 142,857         | 21.4           |
| 8     | 1024    | 266,666         | 40.0           |

## 5. 约束限制

### 5.1 数据库编码问题

Oracle数据库编码设置建议使用UTF8或AL32UTF8，以确保中文等特殊字符的正确处理。

### 5.2 数据一致性

当writeMode为INSERT时，遇到主键冲突将导致任务失败；当writeMode为MERGE时，会覆盖已存在的数据。

### 5.3 权限要求

使用OracleWriter的数据库用户需要具备以下权限：
- 对目标表的INSERT权限
- 对目标表的UPDATE权限（当writeMode为MERGE时）
- 对目标表的SELECT权限（用于获取表结构）

### 5.4 表结构要求

目标表必须事先存在，OracleWriter不会自动创建表。如果表不存在，可以通过preSql在写入前创建表。

## 6. FAQ

**Q: Oracle数据库是否支持写入CLOB/BLOB字段？**

A: 是的，OracleWriter支持写入CLOB字段（从String类型转换）和BLOB字段（从Bytes类型转换）。

**Q: OracleWriter是否支持事务？**

A: 是的，OracleWriter支持事务。默认情况下会在所有数据写入完成后提交事务，也可以通过postSql配置自定义事务行为。

**Q: OracleWriter如何处理NULL值？**

A: OracleWriter会将DataX的NULL值转换为Oracle的NULL值进行插入。

**Q: OracleWriter是否支持自增主键？**

A: 是的，如果Oracle表使用序列作为自增主键，可以在column配置中忽略该字段，或者通过触发器自动填充。