# go-datax MysqlReader

---

## 1 快速介绍

MysqlReader插件实现了从MySQL数据库读取数据的功能。在底层实现上，MysqlReader通过JDBC连接远程MySQL数据库，并根据用户配置的信息生成查询SQL，然后发送到远程MySQL数据库，并将该SQL执行返回结果使用go-datax自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，MysqlReader将其拼接为SQL语句发送到MySQL数据库；对于用户配置querySql信息，MysqlReader直接将其发送到MySQL数据库。

## 2 实现原理

简而言之，MysqlReader通过JDBC连接器连接到远程的MySQL数据库，并根据用户配置的信息生成查询SQL，然后发送到远程MySQL数据库，并将该SQL执行返回结果使用go-datax自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table、Column、Where的信息，MysqlReader将其拼接为SQL语句发送到MySQL数据库；对于用户配置querySql信息，MysqlReader直接将其发送到MySQL数据库。

## 3 功能说明

### 3.1 配置样例

* 配置一个从MySQL数据库同步抽取数据到本地的作业:

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
                        "column": [
                            "id",
                            "name",
                            "age",
                            "email",
                            "created_at"
                        ],
                        "splitPk": "id",
                        "connection": [
                            {
                                "jdbcUrl": [
                                    "jdbc:mysql://localhost:3306/source_db?charset=utf8"
                                ],
                                "table": [
                                    "users"
                                ]
                            }
                        ],
                        "where": "age > 18"
                    }
                },
                "writer": {
                    "name": "mysqlwriter",
                    "parameter": {
                        "username": "root",
                        "password": "password",
                        "column": ["*"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://localhost:3306/target_db?charset=utf8",
                                "table": ["users_adult"]
                            }
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

  * 描述：描述的是到对端数据库的JDBC连接信息，使用JSON的数组描述，并支持一个库填写多个连接地址。之所以使用JSON数组描述连接信息，是因为阿里集团内部支持多个IP探测，如果配置了多个，MysqlReader可以依次探测ip的可连性，直到选择一个合法的IP。如果全部连接失败，MysqlReader报错。 注意，jdbcUrl必须包含在connection配置单元中。对于阿里集团外部使用情况，JSON数组填写一个JDBC连接即可。

    jdbcUrl按照MySQL官方规范，并可以填写连接附加参数信息。具体请参看MySQL官方文档或者咨询对应DBA。

  * 必选：是 <br />

  * 默认值：无 <br />

* **username**

  * 描述：数据源的用户名 <br />

  * 必选：是 <br />

  * 默认值：无 <br />

* **password**

  * 描述：数据源指定用户名的密码 <br />

  * 必选：是 <br />

  * 默认值：无 <br />

* **table**

  * 描述：所选取的需要同步的表名,使用JSON数组描述，因此支持多张表同时抽取。当配置为多张表时，用户自己需保证多张表是同一schema结构，MysqlReader不予检查表是否同一逻辑表。注意，table必须包含在connection配置单元中。<br />

  * 必选：是 <br />

  * 默认值：无 <br />

* **column**

  * 描述：所配置的表中需要同步的列名集合，使用JSON的数组描述字段信息。用户使用\*代表默认使用所有列配置，例如["\*"]。

    支持列裁剪，即列可以挑选部分列进行导出。

    支持列换序，即列可以不按照表schema信息进行导出。

    支持常量配置，用户需要按照MySQL语法格式:
    ["id", "`table`", "1", "'bazhen.csy'", "null", "to_char(a + 1)", "2.3" , "true"]
    id为普通列名，`table`为包含保留字的列名，1为整形数字常量，'bazhen.csy'为字符串常量，null为空指针，to_char(a + 1)为表达式，2.3为浮点数，true为布尔值。

    **🚀 go-datax增强功能**: 当配置为 `["*"]` 时，系统会自动获取表的所有列，确保数据读取的完整性。

  * 必选：是 <br />

  * 默认值：无 <br />

* **splitPk**

  * 描述：MysqlReader进行数据抽取时，如果指定splitPk，表示用户希望使用splitPk代表的字段进行数据分片，go-datax因此会启动并发任务进行数据同步，这样可以大大提供数据同步的效能。

    推荐splitPk用户使用表主键，因为表主键通常情况下比较均匀，因此切分出来的分片也不容易出现数据热点。

    目前splitPk仅支持整形数据切分，不支持浮点、字符串、日期等其他类型。如果用户指定其他非支持类型，MysqlReader将报错！

    splitPk如果不填写，将视作用户不对单表进行切分，MysqlReader使用单通道同步全量数据。

  * 必选：否 <br />

  * 默认值：空 <br />

* **where**

  * 描述：筛选条件，MysqlReader根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > time('2017-07-04 00:00:00')。注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句。<br />

          where条件可以有效地进行业务增量同步。如果不填写where语句，包括不提供where的key或者value，go-datax均视作同步全量数据。

  * 必选：否 <br />

  * 默认值：无 <br />

* **querySql**

  * 描述：在有些业务场景下，where这一配置项不足以描述所筛选的条件，用户可以通过该配置型来自定义筛选SQL。当用户配置了这一项之后，go-datax系统就会忽略table，column，where条件的配置，直接使用这个配置项的内容对数据进行筛选，例如需要进行多表join后同步数据，使用select a,b from table_a join table_b on table_a.id = table_b.id <br />

                 注意：当用户配置querySql时，MysqlReader直接忽略table、column、where条件的配置。

  * 必选：否 <br />

  * 默认值：无 <br />

### 3.3 类型转换

目前MysqlReader支持大部分MySQL类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出MysqlReader针对MySQL类型转换列表:

| go-datax 内部类型| MySQL 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, bigint, year|
| Double   |float, double, decimal |
| String   |varchar, char, tinytext, text, mediumtext, longtext, binary, varbinary, enum, set|
| Date     |date, datetime, timestamp, time |
| Boolean  |bit, bool |
| Bytes    |tinyblob, mediumblob, blob, longblob|

## 4 性能优化

### 4.1 分片读取

使用splitPk配置可以实现并行读取：

```json
{
    "reader": {
        "name": "mysqlreader",
        "parameter": {
            "splitPk": "id",
            // 其他配置...
        }
    }
}
```

配置channel为3时，go-datax会：
1. 查询splitPk字段的最小值和最大值
2. 按范围将数据分为3片
3. 启动3个并发任务同时读取
4. 大大提升数据同步效率

### 4.2 读取优化建议

1. **合理设置splitPk**: 选择分布均匀的整型主键
2. **优化where条件**: 添加索引支持的过滤条件
3. **控制并发数**: 避免对源数据库造成过大压力
4. **字段裁剪**: 只读取需要的列，减少网络传输

## FAQ

***

**Q: MysqlReader同时支持多张表读取吗?**

A: 支持。用户可以在table配置多张表，但需要确保多张表的schema结构相同。go-datax会对每张表使用相同的查询逻辑。

***

**Q: MysqlReader如何处理增量同步?**

A: 通过where条件实现增量同步，例如：
- 基于时间: `"where": "updated_at > '2023-01-01'"`
- 基于ID: `"where": "id > 1000"`
- 结合业务逻辑: `"where": "status = 'active' AND created_at >= CURRENT_DATE"`

***

**Q: splitPk支持哪些数据类型?**

A: 目前只支持整型数据类型，包括：
- int, tinyint, smallint, mediumint, bigint
- year

不支持字符串、浮点数、日期等类型作为splitPk。

***

**Q: 如何处理大表读取性能问题?**

A: 建议：
1. 配置合适的splitPk实现分片读取
2. 增加合理的where条件过滤数据
3. 适当调整channel数量
4. 确保splitPk字段有索引
5. 避免在业务高峰期执行大表同步

***

**Q: querySql和table/column模式有什么区别？**

A:
- **table/column模式**: 适合简单的单表查询，支持splitPk分片
- **querySql模式**: 适合复杂查询（如多表join），但不支持splitPk分片

使用querySql时会忽略table、column、where、splitPk等配置。

***

**Q: 如何处理中文字符编码问题?**

A: 建议在jdbcUrl中添加charset参数：
- `jdbc:mysql://localhost:3306/db?charset=utf8`
- `jdbc:mysql://localhost:3306/db?charset=utf8mb4` (支持emoji等4字节字符)

***