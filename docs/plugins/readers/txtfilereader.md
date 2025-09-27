# TxtFileReader插件文档

## 1. 快速介绍

TxtFileReader插件提供了读取本地文件系统中文本文件的能力。TxtFileReader实现了从本地文件读取数据，转换为DataX传输协议传递给Writer。

本地文件本身是无结构化数据存储，对于DataX而言，TxtFileReader实现上类比于数据库读取，将本地文件作为源表，列作为表字段，基于用户指定的字段信息，读取文本文件。

TxtFileReader支持的功能如下：
1. 支持且仅支持读取TXT的文件，且要求TXT中的schema为一张二维表
2. 支持类CSV格式文件，自定义分隔符
3. 支持多种类型数据读取（Boolean、Long、Double、String、Date），每一种类型可以自定义格式
4. 支持列裁剪，列常量
5. 支持递归读取、支持文件名过滤
6. 支持文件压缩，现有压缩格式为gzip、bzip2、zip

## 2. 实现原理

TxtFileReader根据用户指定的信息，扫描本地文件系统，逐一读取指定的文本文件，并根据用户配置的column信息，逐一解析文本文件的列，转换为DataX传输协议传递给Writer。

## 3. 功能说明

### 3.1 配置样例

```json
{
    "name": "txtfilereader",
    "parameter": {
        "path": ["/Users/longkeyy/case00/data.txt"],
        "encoding": "UTF-8",
        "column": [
            {
                "index": 0,
                "type": "long"
            },
            {
                "index": 1,
                "type": "string"
            },
            {
                "index": 2,
                "type": "date",
                "format": "yyyy-MM-dd"
            }
        ],
        "fieldDelimiter": ",",
        "compress": "gzip",
        "skipHeader": true,
        "nullFormat": "\\N"
    }
}
```

### 3.2 参数说明

#### path
- 描述：本地文件系统的路径信息，注意这里可以支持填写多个路径
- 必选：是
- 默认值：无
- 格式：List<String>或String
- 支持文件名通配符，但只支持*作为文件名的通配符

#### column
- 描述：读取字段列表，type指定源数据的类型，index指定当前字段来自于文本第几列（以0开始），value指定当前类型为常量，不从源头文件读取数据，而是根据value值自动生成对应的列
- 必选：是
- 默认值：无
- 支持类型：Long、Double、String、Boolean、Date，每一种类型可以给出format格式信息

#### fieldDelimiter
- 描述：读取的字段分隔符
- 必选：是
- 默认值：`,`

#### encoding
- 描述：读取文件的编码配置
- 必选：否
- 默认值：UTF-8

#### compress
- 描述：文件压缩类型
- 必选：否
- 默认值：无
- 支持：gzip、bzip2、zip

#### skipHeader
- 描述：类CSV格式文件可能存在表头为标题情况，需要跳过
- 必选：否
- 默认值：false

#### nullFormat
- 描述：文本文件中无法使用标准字符串定义null(空指针)，DataX提供nullFormat定义哪些字符串可以表示为null
- 必选：否
- 默认值：`\N`

#### csvReaderConfig
- 描述：读取CSV类型文件参数配置，Map类型。不配置则使用默认值
- 必选：否
- 默认值：
```json
{
    "safetySwitch": false,
    "skipEmptyRecords": false,
    "useTextQualifier": false
}
```

### 3.3 类型转换

本地文件本身不提供数据类型，该类型是DataX TxtFileReader定义：

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
- 本地准备2个文件，每个文件600万行，总共1200万行
- 文件大小总计约2GB
- 每一行记录大小约为200字节

#### 机器参数
- 执行DataX的机器参数为：8核32G

#### DataX jvm参数
-Xms8G -Xmx8G -XX:+UseG1GC

### 4.2 测试报告

| 通道数| DataX速度(Rec/s)| DataX流量(MB/s) |
|--------|--------| --------|
|1| 43062| 8.9|
|4| 108943| 22.5|
|8| 170025| 35.1|
|16|195918|40.4|
|32|198320|41.0|

## 5. 约束限制

### 5.1 单个File支持情况

目前TxtFileReader仅能支持单文件多线程读取，这里的单文件多线程是指将单个文件进行切分，然后多个线程各自读取文件片段的功能。

### 5.2 压缩文件支持情况

压缩文件当前不能支持多线程并发读取，压缩文件的多线程并发读取需要涉及文件切分，而常见的压缩算法均不支持文件切分读取。

### 5.3 文件编码

TxtFileReader仅支持utf-8编码的文件读取。

## 6. FAQ

**Q: TxtFileReader如何实现断点续传？**

A: TxtFileReader不支持断点续传，重新启动任务将从头开始读取。

**Q: TxtFileReader支持读取FTP文件吗？**

A: TxtFileReader仅支持本地文件系统，不支持FTP、HDFS等远程文件系统。

**Q: TxtFileReader能否读取Excel文件？**

A: TxtFileReader仅支持纯文本文件，不支持Excel等二进制格式文件。