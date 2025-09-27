# go-datax SqliteWriter

---

## 1 快速介绍

SqliteWriter插件实现了写入数据到 SQLite本地数据库文件的功能。在底层实现上，SqliteWriter通过SQLite驱动连接本地 SQLite 数据库文件，并执行相应的 insert into ... 或 replace into ... sql 语句将数据写入 SQLite，内部会分批次提交入库。

SqliteWriter面向ETL开发工程师和数据分析师，他们使用SqliteWriter将数据导入到轻量级的SQLite数据库中进行分析或存储。

## 2 实现原理

SqliteWriter通过 go-datax 框架获取 Reader 生成的协议数据，根据你配置生成相应的SQL插入语句

* `insert into...`(当主键/唯一性索引冲突时会写不进去冲突的行)
* `replace into...`(当主键/唯一性索引冲突时会用新行替换已有行)

<br />

    注意：
    1. SQLite是文件型数据库，确保go-datax进程对目标数据库文件有读写权限。
    2. SQLite支持有限的并发写入，建议使用较少的并发通道数。
    3. SqliteWriter支持配置writeMode参数。

## 3 功能说明

### 3.1 配置样例

* 这里使用一份从PostgreSQL到SQLite的数据同步示例。

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
                        "column": ["*"],
                        "connection": [
                            {
                                "jdbcUrl": ["jdbc:postgresql://localhost:5432/source_db"],
                                "table": ["users"]
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "sqlitewriter",
                    "parameter": {
                        "column": ["*"],
                        "writeMode": "replace",
                        "batchSize": 500,
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:sqlite:/path/to/target.db",
                                "table": ["users"]
                            }
                        ],
                        "preSql": [
                            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, email TEXT, created_at TEXT)"
                        ]
                    }
                }
            }
        ]
    }
}
```

### 3.2 参数说明

* **jdbcUrl**

    * 描述：目的数据库的 JDBC 连接信息，对于SQLite格式为 `jdbc:sqlite:/path/to/database.db`，jdbcUrl必须包含在connection配置单元中。

      注意：1、SQLite每个连接对应一个数据库文件。
      2、路径可以是相对路径或绝对路径，确保go-datax进程有读写权限。

  * 必选：是 <br />

  * 默认值：无 <br />

* **table**

  * 描述：目的表的表名称。支持写入一个或者多个表。当配置为多张表时，必须确保所有表结构保持一致。

               注意：table 和 jdbcUrl 必须包含在 connection 配置单元中

  * 必选：是 <br />

  * 默认值：无 <br />

* **column**

  * 描述：目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。如果要依次写入全部列，使用\*表示, 例如: "column": ["\*"]

    **🚀 go-datax增强功能**: 当配置为 `["*"]` 时，系统会自动查询目标表结构并获取所有列名，确保INSERT语句的正确性。

               注意：1、我们强烈不推荐你这样配置，因为当你目的表字段个数、类型等有改动时，你的任务可能运行不正确或者失败
                    2、此处 column 不能配置任何常量值

  * 必选：是 <br />

  * 默认值：否 <br />

* **writeMode**

  * 描述：控制写入数据到目标表采用 `insert into` 或者 `replace into` 语句<br />

  * 必选：否 <br />

  * 所有选项：insert/replace <br />

  * 默认值：insert <br />

* **preSql**

  * 描述：写入数据到目的表前，会先执行这里的标准语句。常用于创建表或清理数据。<br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **postSql**

  * 描述：写入数据到目的表后，会执行这里的标准语句。可用于创建索引等操作。<br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少go-datax与SQLite的交互次数，并提升整体吞吐量。由于SQLite的特性，建议设置较小的值。<br />

	* 必选：否 <br />

	* 默认值：1024 <br />

### 3.3 类型转换

SQLite使用动态类型系统，支持以下存储类：

下面列出 SqliteWriter针对 SQLite类型转换列表:

| go-datax 内部类型| SQLite 存储类    | 说明 |
| -------- | -----  | ----- |
| Long     |INTEGER | 有符号整数，根据值大小存储在1,2,3,4,6或8字节中 |
| Double   |REAL | 8字节IEEE浮点数 |
| String   |TEXT | 文本字符串，使用数据库编码存储 |
| Date     |TEXT | 日期时间格式化为文本存储 |
| Boolean  |INTEGER | 0(false) 或 1(true) |
| Bytes    |BLOB | 二进制数据，原样存储 |

## 4 go-datax 增强功能

### 4.1 智能列名解析

当配置 `"column": ["*"]` 时，go-datax会：
1. 自动连接目标数据库文件
2. 使用 `PRAGMA table_info()` 获取表的实际列结构
3. 按列顺序构建正确的INSERT/REPLACE语句

### 4.2 写入模式支持

- `insert`: 标准插入模式，主键冲突时插入失败
- `replace`: 替换模式，主键冲突时替换原有数据

### 4.3 文件数据库优化

- 自动处理SQLite文件路径
- 优化的批量插入性能
- 适合轻量级数据存储和分析场景

## 4 使用场景

### 4.1 数据分析

SQLite非常适合作为数据分析的中间存储：
- 轻量级，无需安装数据库服务器
- 支持标准SQL查询
- 文件便于分发和备份

### 4.2 数据归档

将历史数据从生产数据库导出到SQLite文件：
- 减轻生产数据库压力
- 便于长期存储和查询
- 支持压缩存储

### 4.3 本地开发

开发环境的数据模拟：
- 快速创建测试数据
- 无需复杂的数据库环境
- 易于版本控制

## FAQ

***

**Q: SQLite数据库文件不存在时会自动创建吗?**

A: 是的，如果指定的数据库文件不存在，SQLite会自动创建。但请确保目录存在且go-datax进程有写权限。

***

**Q: SQLite支持多少并发写入?**

A: SQLite同时只支持一个写入事务，建议将channel设置为1，或使用较小的并发数以避免锁等待。

***

**Q: 如何处理大量数据写入SQLite?**

A: 建议：
1. 设置适当的batchSize（如500-1000）
2. 使用WAL模式提升并发性能
3. 考虑在preSql中设置 `PRAGMA synchronous = NORMAL`

***

**Q: SQLite数据库文件过大怎么办?**

A: 可以考虑：
1. 使用VACUUM命令压缩数据库
2. 分表存储大量数据
3. 定期归档历史数据到新文件

***