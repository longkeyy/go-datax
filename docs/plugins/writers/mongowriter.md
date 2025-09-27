# MongoWriter插件文档

## 1. 快速介绍

MongoWriter插件实现了向MongoDB写入数据的功能。在底层实现上，MongoWriter通过官方MongoDB Go驱动，连接远程MongoDB数据库，并执行相应的批量写入操作。

## 2. 实现原理

MongoWriter通过MongoDB Go驱动连接远程MongoDB数据库，并根据用户配置的信息生成文档，将数据写入MongoDB集合中。MongoWriter会根据配置的写入模式选择不同的写入策略：
- insert模式：直接插入新文档
- replace模式：根据业务主键进行替换或插入（upsert）

## 3. 功能说明

### 3.1 配置样例

以下是一个向MongoDB数据库写入数据的配置范例：

```json
{
    "name": "mongowriter",
    "parameter": {
        "address": ["127.0.0.1:27017"],
        "userName": "user",
        "userPassword": "password",
        "dbName": "test",
        "collectionName": "users",
        "column": [
            {
                "name": "_id",
                "type": "ObjectId"
            },
            {
                "name": "name",
                "type": "string"
            },
            {
                "name": "age",
                "type": "long"
            },
            {
                "name": "tags",
                "type": "array",
                "splitter": ",",
                "itemtype": "string"
            }
        ],
        "writeMode": {
            "isReplace": "false"
        }
    }
}
```

### 3.2 参数说明

#### address
- 描述：MongoDB的连接地址信息
- 必选：是
- 格式：List<String>，每个地址格式为host:port
- 默认值：无

#### userName
- 描述：MongoDB的用户名
- 必选：是
- 默认值：无

#### userPassword
- 描述：MongoDB的密码
- 必选：是
- 默认值：无

#### dbName
- 描述：MongoDB的数据库名
- 必选：是
- 默认值：无

#### collectionName
- 描述：MongoDB的集合名
- 必选：是
- 默认值：无

#### column
- 描述：所配置的列信息
- 必选：是
- 格式：Array<Object>，每个Object包含name、type、splitter、itemtype字段
- 默认值：无

#### writeMode
- 描述：写入模式配置
- 必选：否
- 格式：Object，包含isReplace和replaceKey字段
- 默认值：{"isReplace": "false"}

#### writeMode.isReplace
- 描述：是否开启replace模式
- 必选：否
- 格式：String，取值为"true"或"false"
- 默认值："false"

#### writeMode.replaceKey
- 描述：replace模式下的业务主键字段名
- 必选：当isReplace为true时必选
- 格式：String
- 默认值：无

### 3.3 类型转换

MongoWriter支持大部分MongoDB类型的写入。下面列出MongoWriter针对MongoDB数据类型转换列表：

| DataX内部类型 | MongoDB数据类型               |
|------------|---------------------------|
| Long       | int, long                 |
| Double     | double                    |
| String     | string                    |
| Date       | date                      |
| Boolean    | boolean                   |
| Bytes      | bytes                     |
| Array      | array (通过splitter从字符串转换) |
| ObjectId   | ObjectId                  |

### 3.4 写入模式

#### insert模式
默认的写入模式，直接向集合插入新文档。如果遇到_id冲突会报错。

#### replace模式
根据配置的replaceKey字段进行upsert操作：
- 如果文档存在（根据replaceKey判断），则替换整个文档
- 如果文档不存在，则插入新文档

### 3.5 preSql支持

MongoWriter支持在写入前执行预处理操作：

- `drop`：删除整个集合
- `remove`：删除匹配条件的文档

配置示例：
```json
"preSql": ["drop"]
```

## 4. 约束限制

### 4.1 数据类型限制

目前MongoWriter支持的数据类型包括：int、long、double、string、date、boolean、bytes、array、ObjectId。

### 4.2 性能限制

MongoWriter采用批量写入模式，默认批量大小为1000。在大数据量写入时建议：
1. 合理设置批量大小
2. 确保目标集合有合适的索引
3. 避免频繁的replace操作

### 4.3 事务限制

MongoWriter不支持跨文档事务，每个批次的写入是原子的，但整个任务不保证事务一致性。

## 5. FAQ

**Q: MongoWriter如何处理写入冲突？**

A: 在insert模式下，遇到_id冲突会报错；在replace模式下，会根据replaceKey进行upsert操作。

**Q: MongoWriter支持分片集合吗？**

A: 是的，MongoWriter支持向分片集合写入数据，但需要确保分片键包含在写入数据中。

**Q: MongoWriter的性能如何？**

A: MongoWriter使用批量操作来提高性能，在网络良好的情况下可以达到较高的写入吞吐量。