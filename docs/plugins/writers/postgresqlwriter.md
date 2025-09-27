# go-datax PostgresqlWriter

> 本文档基于阿里巴巴DataX项目的PostgreSQL Writer文档，适配go-datax实现

---

## 1 快速介绍

PostgresqlWriter插件实现了写入数据到 PostgreSQL主库目的表的功能。在底层实现上，PostgresqlWriter通过JDBC连接远程 PostgreSQL 数据库，并执行相应的 insert into ... sql 语句将数据写入 PostgreSQL，内部会分批次提交入库。

PostgresqlWriter面向ETL开发工程师，他们使用PostgresqlWriter从数仓导入数据到PostgreSQL。同时 PostgresqlWriter亦可以作为数据迁移工具为DBA等用户提供服务。

## 2 实现原理

PostgresqlWriter通过 go-datax 框架获取 Reader 生成的协议数据，根据你配置生成相应的SQL插入语句

* `insert into...`(当主键/唯一性索引冲突时会写不进去冲突的行)

<br />

    注意：
    1. 目的表所在数据库必须是主库才能写入数据；整个任务至少需具备 insert into...的权限，是否需要其他权限，取决于你任务配置中在 preSql 和 postSql 中指定的语句。
    2. PostgresqlWriter和MysqlWriter不同，不支持配置writeMode参数。

## 3 功能说明

### 3.1 配置样例

* 这里使用一份从PostgreSQL到PostgreSQL的数据同步示例。

```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 3
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
                        "splitPk": "id",
                        "connection": [
                            {
                                "jdbcUrl": ["jdbc:postgresql://localhost:5432/source_db"],
                                "table": ["users"]
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "postgresqlwriter",
                    "parameter": {
                        "username": "postgres",
                        "password": "postgres",
                        "column": ["*"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:postgresql://localhost:5432/target_db",
                                "table": ["users_copy"]
                            }
                        ],
                        "preSql": [
                            "DROP TABLE IF EXISTS users_copy",
                            "CREATE TABLE users_copy (id SERIAL PRIMARY KEY, name VARCHAR(255), age INTEGER, email VARCHAR(255), created_at TIMESTAMP)"
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

    * 描述：目的数据库的 JDBC 连接信息 ,jdbcUrl必须包含在connection配置单元中。

      注意：1、在一个数据库上只能配置一个值。
      2、jdbcUrl按照PostgreSQL官方规范，并可以填写连接附加参数信息。具体请参看PostgreSQL官方文档或者咨询对应 DBA。

  * 必选：是 <br />

  * 默认值：无 <br />

* **username**

  * 描述：目的数据库的用户名 <br />

  * 必选：是 <br />

  * 默认值：无 <br />

* **password**

  * 描述：目的数据库的密码 <br />

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

* **preSql**

  * 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，请使用 `@table` 表示，这样在实际执行 Sql 语句时，会对变量按照实际表名称进行替换。比如你的任务是要写入到目的端的100个同构分表(表名称为:datax_00,datax01, ... datax_98,datax_99)，并且你希望导入数据前，先对表中数据进行删除操作，那么你可以这样配置：`"preSql":["delete from @table"]`，效果是：在执行到每个表写入数据前，会先执行对应的 delete from 对应表名称 <br />

    **⚠️ 当前限制**: go-datax暂不支持 `@table` 变量替换功能，请直接使用具体的表名。

  * 必选：否 <br />

  * 默认值：无 <br />

* **postSql**

  * 描述：写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ） <br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少go-datax与PostgreSQL的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成go-datax运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1024 <br />

### 3.3 类型转换

目前 PostgresqlWriter支持大部分 PostgreSQL类型，但也存在部分没有支持的情况，请注意检查你的类型。

下面列出 PostgresqlWriter针对 PostgreSQL类型转换列表:

| go-datax 内部类型| PostgreSQL 数据类型    |
| -------- | -----  |
| Long     |bigint, bigserial, integer, smallint, serial |
| Double   |double precision, money, numeric, real |
| String   |varchar, char, text, bit|
| Date     |date, time, timestamp |
| Boolean  |bool|
| Bytes    |bytea|

## 4 go-datax 增强功能

相比原版DataX，go-datax PostgresqlWriter提供了以下增强功能：

### 4.1 智能列名解析

当配置 `"column": ["*"]` 时，go-datax会：
1. 自动连接目标数据库
2. 查询 `information_schema.columns` 获取表的实际列结构
3. 按列顺序构建正确的INSERT语句

### 4.2 现代化架构

- 使用GORM ORM框架进行数据库操作
- 更好的连接池管理
- 改进的错误处理和日志记录

### 4.3 并发优化

- 支持多通道并行写入
- 优化的批量插入性能
- 更好的资源管理

## FAQ

***

**Q: PostgresqlWriter 执行 postSql 语句报错，那么数据导入到目标数据库了吗?**

A: go-datax 导入过程存在三块逻辑，pre 操作、导入操作、post 操作，其中任意一环报错，go-datax 作业报错。由于 go-datax 不能保证在同一个事务完成上述几个操作，因此有可能数据已经落入到目标端。

***

**Q: 按照上述说法，那么有部分脏数据导入数据库，如果影响到线上数据库怎么办?**

A: 目前有两种解法，第一种配置 pre 语句，该 sql 可以清理当天导入数据， go-datax 每次导入时候可以把上次清理干净并导入完整数据。
第二种，向临时表导入数据，完成后再 rename 到线上表。

***

**Q: column配置为["*"]时出现列不匹配错误怎么办？**

A: go-datax会自动解析目标表列结构，如果出现列不匹配，请检查：
1. 目标表是否存在
2. 数据库连接权限是否足够
3. Reader读取的列数量是否与目标表列数量一致
4. 建议先使用明确的列名配置进行测试

***