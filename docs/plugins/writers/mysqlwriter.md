# go-datax MysqlWriter

---

## 1 快速介绍

MysqlWriter插件实现了写入数据到 MySQL主库目的表的功能。在底层实现上，MysqlWriter通过JDBC连接远程 MySQL 数据库，并执行相应的 insert into ... 或 replace into ... sql 语句将数据写入 MySQL，内部会分批次提交入库。

MysqlWriter面向ETL开发工程师，他们使用MysqlWriter从数仓导入数据到MySQL。同时 MysqlWriter亦可以作为数据迁移工具为DBA等用户提供服务。

## 2 实现原理

MysqlWriter通过 go-datax 框架获取 Reader 生成的协议数据，根据你配置生成相应的SQL插入语句

* `insert into...`(当主键/唯一性索引冲突时会写不进去冲突的行)
* `replace into...`(当主键/唯一性索引冲突时会用新行替换已有行)

<br />

    注意：
    1. 目的表所在数据库必须是主库才能写入数据；整个任务至少需具备 insert/replace 权限，是否需要其他权限，取决于你任务配置中在 preSql 和 postSql 中指定的语句。
    2. MysqlWriter支持配置writeMode参数。

## 3 功能说明

### 3.1 配置样例

* 这里使用一份从MySQL到MySQL的数据同步示例。

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
                    "name": "mysqlreader",
                    "parameter": {
                        "username": "root",
                        "password": "password",
                        "column": ["*"],
                        "splitPk": "id",
                        "connection": [
                            {
                                "jdbcUrl": ["jdbc:mysql://localhost:3306/source_db?charset=utf8"],
                                "table": ["users"]
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "mysqlwriter",
                    "parameter": {
                        "username": "root",
                        "password": "password",
                        "column": ["*"],
                        "writeMode": "insert",
                        "batchSize": 1024,
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://localhost:3306/target_db?charset=utf8",
                                "table": ["users_copy"]
                            }
                        ],
                        "preSql": [
                            "DROP TABLE IF EXISTS users_copy",
                            "CREATE TABLE users_copy LIKE users"
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
      2、jdbcUrl按照MySQL官方规范，并可以填写连接附加参数信息。建议使用charset=utf8。

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

* **writeMode**

  * 描述：控制写入数据到目标表采用 `insert into` 或者 `replace into` 语句<br />

  * 必选：否 <br />

  * 所有选项：insert/replace <br />

  * 默认值：insert <br />

* **preSql**

  * 描述：写入数据到目的表前，会先执行这里的标准语句。<br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **postSql**

  * 描述：写入数据到目的表后，会执行这里的标准语句。<br />

  * 必选：否 <br />

  * 默认值：无 <br />

* **batchSize**

	* 描述：一次性批量提交的记录数大小，该值可以极大减少go-datax与MySQL的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成go-datax运行进程OOM情况。<br />

	* 必选：否 <br />

	* 默认值：1024 <br />

### 3.3 类型转换

目前 MysqlWriter支持大部分 MySQL类型，但也存在部分没有支持的情况，请注意检查你的类型。

下面列出 MysqlWriter针对 MySQL类型转换列表:

| go-datax 内部类型| MySQL 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, bigint |
| Double   |float, double, decimal |
| String   |varchar, char, tinytext, text, mediumtext, longtext|
| Date     |date, datetime, timestamp, time, year |
| Boolean  |bit, bool |
| Bytes    |tinyblob, mediumblob, blob, longblob, binary, varbinary |

## 4 go-datax 增强功能

### 4.1 智能列名解析

当配置 `"column": ["*"]` 时，go-datax会：
1. 自动连接目标数据库
2. 查询 `information_schema.columns` 获取表的实际列结构
3. 按列顺序构建正确的INSERT/REPLACE语句

### 4.2 写入模式支持

- `insert`: 标准插入模式，主键冲突时插入失败
- `replace`: 替换模式，主键冲突时替换原有数据

### 4.3 现代化架构

- 使用GORM ORM框架进行数据库操作
- MySQL驱动自动处理字符编码
- 优化的连接池管理和批量插入性能

## FAQ

***

**Q: writeMode配置为replace时有什么注意事项?**

A: replace模式会在主键冲突时删除原有行并插入新行，这意味着：
1. 目标表必须有主键或唯一索引
2. 可能会影响自增主键的连续性
3. 相比insert模式性能略低

***

**Q: 如何处理中文字符编码问题?**

A: 建议在jdbcUrl中添加charset参数：
- `jdbc:mysql://localhost:3306/db?charset=utf8`
- `jdbc:mysql://localhost:3306/db?charset=utf8mb4` (支持emoji等4字节字符)

***