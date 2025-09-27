# TxtFileWriter插件文档

## 1. 快速介绍

TxtFileWriter插件提供了向本地文件系统写入类CSV格式的一维表文件的能力。TxtFileWriter根据用户的配置，将DataX读取的数据写入本地文件系统。

TxtFileWriter支持的功能如下：
1. 支持配置写入的字段分隔符，默认为","
2. 支持配置文件写入编码，默认为UTF-8
3. 支持配置压缩，现有压缩格式为gzip、bzip2、zip、lzo、lzop、tgz
4. 支持配置文件写入模式：追加、覆盖、非冲突写入模式（文件存在时报错）

## 2. 实现原理

TxtFileWriter通过DataX传输协议，接收Writer端数据，并将数据写入本地文件系统。

在写入本地文件系统时，首先根据用户配置的path、fileName等信息，确定写入的文件路径，然后根据writeMode配置，选择写入策略，最后根据encoding、fieldDelimiter等信息，将DataX传输的数据转换为符合配置格式的字符串，写入到本地文件中。

## 3. 功能说明

### 3.1 配置样例

```json
{
    "name": "txtfilewriter",
    "parameter": {
        "path": "/Users/longkeyy/case00/",
        "fileName": "datax_result",
        "writeMode": "truncate",
        "fieldDelimiter": ",",
        "encoding": "UTF-8",
        "nullFormat": "\\N",
        "dateFormat": "yyyy-MM-dd",
        "fileFormat": "csv",
        "header": ["id", "name", "age"],
        "compress": "gzip"
    }
}
```

### 3.2 参数说明

#### path
- 描述：本地文件系统的路径信息，注意这里的路径应该是目录，而不是文件
- 必选：是
- 默认值：无

#### fileName
- 描述：TxtFileWriter写入的文件名，该文件名会添加随机的后缀作为每个线程写入的文件名
- 必选：是
- 默认值：无

#### writeMode
- 描述：TxtFileWriter写入前数据清理处理模式
- 必选：是
- 默认值：无
- 可选值：
  - truncate：写入前清理目录下一fileName为前缀的所有文件
  - append：写入前不做任何处理，DataX TxtFileWriter直接使用filename写入，并保证文件名不冲突
  - nonConflict：如果目录下有fileName前缀的文件，直接报错

#### fieldDelimiter
- 描述：写入的字段分隔符
- 必选：否
- 默认值：`,`

#### encoding
- 描述：写入文件的编码配置
- 必选：否
- 默认值：UTF-8

#### nullFormat
- 描述：文本文件中无法使用标准字符串定义null(空指针)，DataX提供nullFormat定义哪些字符串可以表示为null
- 必选：否
- 默认值：`\N`

#### dateFormat
- 描述：日期类型的数据序列化到文件中时的格式，例如 "yyyy-MM-dd"
- 必选：否
- 默认值：无

#### fileFormat
- 描述：文件写入的格式，包括csv和text两种
- 必选：否
- 默认值：text
- 说明：
  - csv：严格的csv格式，如果字段中包含列分隔符，会按照csv的转义语法，即字段使用双引号包装，字段中如果有双引号，会使用两个双引号表示转义
  - text：简单的分隔符格式，如果字段中包含列分隔符，就直接写入，不会作转义处理

#### header
- 描述：TxtFileWriter写入时的表头，例如["id","name","age"]
- 必选：否
- 默认值：无

#### compress
- 描述：文件压缩类型
- 必选：否
- 默认值：无
- 支持：gzip、bzip2、zip、lzo、lzop、tgz

#### suffix
- 描述：写入的文件后缀，例如：".csv"、".txt"等
- 必选：否
- 默认值：无

### 3.3 类型转换

本地文件本身不提供数据类型，该类型是DataX TxtFileWriter定义：

| DataX 内部类型| 本地文件 数据类型    |
| -------- | -----  |
| Long     |Long |
| Double   |Double|
| String   |String|
| Boolean  |Boolean |
| Date     |Date |

其中：
- 本地文件 Long是指本地文件文本中使用整形的字符串表示形式，例如"19901219"
- 本地文件 Double是指本地文件文本中使用Double的字符串表示形式，例如"3.1415"
- 本地文件 Boolean是指本地文件文本中使用Boolean的字符串表示形式，例如"true"、"false"，不区分大小写
- 本地文件 Date是指本地文件文本中使用Date的字符串表示形式，例如"2014-12-31"，Date可以指定format格式

## 4. 性能报告

### 4.1 环境准备

#### 数据特征
- 2000万行数据，每行大小约为300字节
- 总数据大小约为6GB

#### 机器参数
- 执行DataX的机器参数为：8核32G

#### DataX jvm参数
-Xms8G -Xmx8G -XX:+UseG1GC

### 4.2 测试报告

| 通道数| DataX速度(Rec/s)| DataX流量(MB/s) |
|--------|--------| --------|
|1| 52486| 15.9|
|4| 169492| 51.4|
|8| 243768| 74.0|
|16|306060|92.8|
|32|315789|95.8|

## 5. 约束限制

### 5.1 文件个数

一个线程负责写入一个文件，DataX会根据通道数量和表配置的分片信息，确定写入的文件个数。每个文件名为：fileName_随机字符串.suffix

### 5.2 文件大小

单个文件大小不能超过100GB（受限于本地文件系统）

### 5.3 中文编码

目前仅支持utf-8文件编码。

## 6. FAQ

**Q: TxtFileWriter是否支持写入时压缩？**

A: 是，TxtFileWriter支持gzip、bzip2、zip等多种压缩格式。

**Q: TxtFileWriter如何保证数据完整性？**

A: TxtFileWriter先将数据写入临时文件，待数据写入完成后再重命名为最终文件名，保证数据完整性。

**Q: TxtFileWriter支持增量写入吗？**

A: TxtFileWriter支持append模式，可以在现有文件基础上追加数据。

**Q: TxtFileWriter如何处理字段中包含分隔符的情况？**

A: 当fileFormat配置为csv时，TxtFileWriter会对包含分隔符的字段进行转义处理；当配置为text时，会直接写入不做转义。