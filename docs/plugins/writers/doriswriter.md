# DorisWriter插件文档

## 1. 快速介绍

DorisWriter插件实现了向Apache Doris分析型数据库写入数据的功能。在底层实现上，DorisWriter通过Stream Load导入方式，将数据写入Doris数据库。Stream Load是Doris提供的一种高效的批量数据导入方式，特别适合大数据量的ETL场景。

DorisWriter支持两种数据格式：CSV和JSON，用户可以根据需要选择合适的格式。

## 2. 实现原理

DorisWriter采用Doris的Stream Load功能进行数据写入。Stream Load通过HTTP协议将数据流式传输到Doris FE（Frontend）节点，然后由FE节点分发给相应的BE（Backend）节点进行数据写入。

整个过程分为以下几个步骤：
1. 将DataX的数据记录转换为CSV或JSON格式
2. 批量收集数据到指定的大小或条数
3. 通过HTTP PUT请求将数据发送到Doris的Stream Load接口
4. 等待Doris返回导入结果并处理错误

## 3. 功能说明

### 3.1 配置样例

以下是一个向Doris数据库写入数据的配置范例：

```json
{
    "name": "doriswriter",
    "parameter": {
        "loadUrl": ["127.0.0.1:8030"],
        "username": "root",
        "password": "password",
        "connection": [
            {
                "jdbcUrl": "jdbc:mysql://127.0.0.1:9030/test_db",
                "selectedDatabase": "test_db",
                "table": ["test_table"]
            }
        ],
        "column": ["id", "name", "age", "email", "created_at"],
        "loadProps": {
            "format": "json",
            "strip_outer_array": true,
            "jsonpaths": "[\"$.id\", \"$.name\", \"$.age\", \"$.email\", \"$.created_at\"]"
        },
        "maxBatchRows": 500000,
        "batchSize": 104857600,
        "flushInterval": 30000
    }
}
```

### 3.2 参数说明

#### loadUrl
- 描述：Doris FE节点的HTTP服务地址，用于Stream Load
- 必选：是
- 格式：List<String>，格式为ip:port，支持多个地址
- 默认值：无

#### username
- 描述：Doris数据库的用户名
- 必选：是
- 默认值：无

#### password
- 描述：Doris数据库的密码
- 必选：是
- 默认值：无

#### jdbcUrl
- 描述：Doris数据库的JDBC连接，用于执行preSql和postSql
- 必选：是
- 格式：jdbc:mysql://ip:port/database
- 默认值：无

#### selectedDatabase
- 描述：需要写入的Doris数据库名
- 必选：是
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

#### loadProps
- 描述：Stream Load的属性配置，会作为HTTP header传递给Doris
- 必选：否
- 格式：Map<String, Object>
- 默认值：{"format": "csv", "column_separator": "\\t", "line_delimiter": "\\n"}

#### maxBatchRows
- 描述：每批次最大行数
- 必选：否
- 格式：Long
- 默认值：500000

#### batchSize
- 描述：每批次最大字节数，单位为字节
- 必选：否
- 格式：Long
- 默认值：104857600（100MB）

#### flushInterval
- 描述：批次刷新时间间隔，单位为毫秒
- 必选：否
- 格式：Long
- 默认值：30000（30秒）

#### maxRetries
- 描述：Stream Load失败时的最大重试次数
- 必选：否
- 格式：Int
- 默认值：3

#### labelPrefix
- 描述：Stream Load的Label前缀，用于保证数据的幂等性
- 必选：否
- 格式：String
- 默认值：datax_doris_writer_

### 3.3 loadProps配置说明

loadProps中的配置会作为HTTP header传递给Doris的Stream Load接口，常用配置如下：

#### CSV格式配置
```json
"loadProps": {
    "format": "csv",
    "column_separator": "\\t",
    "line_delimiter": "\\n",
    "columns": "id,name,age,email,created_at"
}
```

#### JSON格式配置
```json
"loadProps": {
    "format": "json",
    "strip_outer_array": true,
    "jsonpaths": "[\"$.id\", \"$.name\", \"$.age\", \"$.email\", \"$.created_at\"]"
}
```

### 3.4 类型转换

DorisWriter针对Doris数据类型转换列表：

| DataX内部类型 | Doris数据类型                           |
|------------|---------------------------------------|
| Long       | int, tinyint, smallint, int, bigint, largint |
| Double     | float, double, decimal                |
| String     | varchar, char, text, string           |
| Date       | date, datetime                        |
| Boolean    | boolean                               |

### 3.5 写入模式

DorisWriter通过Stream Load实现批量写入，支持以下特性：

#### 幂等性
通过唯一的Label确保相同的数据不会被重复导入。

#### 事务性
每个Stream Load请求都是一个独立的事务，要么全部成功，要么全部失败。

#### 高性能
采用批量写入和异步处理，能够达到很高的写入吞吐量。

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

单行记录大小约为100字节。

#### 机器参数
- 执行DataX的机器参数为：1台8核64G
- Doris集群配置：3FE + 3BE，每台BE为8核32G

#### DataX JVM参数
-Xms8192m -Xmx8192m -XX:+UseG1GC

### 4.2 测试报告

| 通道数 | 批次大小 | DataX速度(Rec/s) | DataX流量(MB/s) | 说明 |
|-------|---------|------------------|-----------------|------|
| 1     | 100MB   | 156,250         | 15.69          | JSON格式 |
| 4     | 100MB   | 512,820         | 51.54          | JSON格式 |
| 8     | 100MB   | 847,457         | 85.18          | JSON格式 |
| 1     | 100MB   | 178,571         | 17.95          | CSV格式 |
| 4     | 100MB   | 625,000         | 62.85          | CSV格式 |
| 8     | 100MB   | 1,020,408       | 102.61         | CSV格式 |

## 5. 约束限制

### 5.1 网络连接
DorisWriter需要能够访问Doris FE节点的HTTP端口（默认8030）和MySQL端口（默认9030）。

### 5.2 数据格式
- 目前支持CSV和JSON两种格式
- JSON格式支持嵌套对象，但建议简单结构以获得更好的性能
- CSV格式性能更好，建议在对性能要求高的场景下使用

### 5.3 事务限制
- 每个Stream Load是一个事务，建议合理设置批次大小
- 过大的批次可能导致事务超时，过小的批次会影响性能

### 5.4 Label限制
- Label必须在Doris集群内唯一
- Label有时效性，过期后可以重复使用
- 建议使用有意义的Label前缀便于管理

## 6. FAQ

**Q: DorisWriter如何保证数据不重复？**

A: DorisWriter通过Stream Load的Label机制保证幂等性，相同Label的数据只会被导入一次。

**Q: DorisWriter支持哪些数据格式？**

A: DorisWriter支持CSV和JSON两种格式，可以通过loadProps.format参数配置。

**Q: DorisWriter的性能如何优化？**

A: 可以通过以下方式优化性能：
1. 增加通道数（channel）
2. 调整批次大小（batchSize）
3. 使用CSV格式而非JSON格式
4. 合理设置Doris表的分桶数

**Q: Stream Load失败怎么办？**

A: DorisWriter会自动重试失败的Stream Load请求，重试次数可以通过maxRetries参数配置。如果重试仍然失败，任务会终止并报告错误信息。