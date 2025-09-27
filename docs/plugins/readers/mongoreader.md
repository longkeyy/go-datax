# MongoReader插件文档

## 1. 快速介绍

MongoReader插件实现了从MongoDB读取数据的功能。在底层实现上，MongoReader通过官方MongoDB Go驱动，连接远程MongoDB数据库，并根据用户配置的查询条件选择数据，将数据以DataX传输协议传递给Writer。

## 2. 实现原理

MongoReader通过MongoDB Go驱动连接远程MongoDB数据库，并根据用户配置的信息生成查询语句，然后发送到远程MongoDB数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置Table中的column信息，MongoReader将该配置信息转换为MongoDB的字段选择，可以理解为：

```json
column: ["id", "name", "age"]
```

当用户配置了查询条件时，MongoReader会将查询条件转换为MongoDB的查询文档。

## 3. 功能说明

### 3.1 配置样例

以下是一个从MongoDB数据库读取数据的配置范例：

```json
{
    "name": "mongoreader",
    "parameter": {
        "address": ["127.0.0.1:27017"],
        "userName": "",
        "userPassword": "",
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
                "splitter": ","
            }
        ],
        "query": "{\"status\": \"active\"}"
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
- 必选：否
- 默认值：无

#### userPassword
- 描述：MongoDB的密码
- 必选：否
- 默认值：无

#### dbName
- 描述：MongoDB的数据库名
- 必选：是
- 默认值：无

#### authDb
- 描述：MongoDB的认证数据库名
- 必选：否
- 默认值：dbName

#### collectionName
- 描述：MongoDB的集合名
- 必选：是
- 默认值：无

#### column
- 描述：所配置的列信息
- 必选：是
- 格式：Array<Object>，每个Object包含name、type、splitter字段
- 默认值：无

#### query
- 描述：MongoDB的查询条件，JSON格式
- 必选：否
- 默认值：无

#### batchSize
- 描述：批量读取大小
- 必选：否
- 默认值：1000

### 3.3 类型转换

MongoReader支持大部分MongoDB类型，但也存在部分限制，请注意检查你的类型。

下面列出MongoReader针对MongoDB数据类型转换列表：

| DataX内部类型 | MongoDB数据类型               |
|------------|---------------------------|
| Long       | int, long                 |
| Double     | double                    |
| String     | string                    |
| Date       | date                      |
| Boolean    | boolean                   |
| Bytes      | bytes                     |
| Array      | array (通过splitter转换为字符串) |

## 4. 约束限制

### 4.1 数据类型限制

目前MongoReader支持的数据类型包括：int、long、double、string、date、boolean、bytes、array。

不支持的数据类型包括：ObjectId以外的复杂类型、嵌套文档的深度查询等。

### 4.2 性能限制

为了防止对MongoDB数据库造成过大压力，MongoReader会在查询时进行适当的批量处理。

## 5. FAQ

**Q: MongoReader支持MongoDB的所有版本吗？**

A: MongoReader基于MongoDB官方Go驱动实现，支持MongoDB 3.6及以上版本。

**Q: MongoReader如何处理嵌套文档？**

A: MongoReader支持通过点号表示法访问嵌套字段，如"user.profile.name"。

**Q: MongoReader是否支持分片集群？**

A: 是的，MongoReader支持MongoDB分片集群，通过配置多个mongos地址即可。