# 数据转换(Transformer)模块

go-datax支持在数据传输过程中进行实时转换，通过内置的Transformer机制对数据进行转换和过滤。

## 功能概述

Transformer在数据流中的位置：
```
Reader → Transformer Chain → Writer
         ↓
  filter → substr → replace → pad → digest
```

每个Transformer按配置顺序依次执行，支持链式处理。

## 内置Transformer

### dx_filter - 数据过滤
根据条件过滤记录，支持多种比较操作。

**支持的操作符**：
- 比较操作：`>`, `<`, `=`, `!=`, `>=`, `<=`
- 模式匹配：`like`, `not like`

**配置示例**：
```json
{
  "name": "dx_filter",
  "parameter": {
    "columnIndex": 2,
    "paras": [">", "18"]
  }
}
```

**使用场景**：
- 过滤年龄大于18的用户记录
- 排除状态为"已删除"的数据
- 根据时间范围筛选数据

### dx_substr - 字符串截取
从指定列截取子字符串。

**参数说明**：
- `paras[0]`: 起始位置（从0开始）
- `paras[1]`: 截取长度

**配置示例**：
```json
{
  "name": "dx_substr",
  "parameter": {
    "columnIndex": 1,
    "paras": ["0", "10"]
  }
}
```

**使用场景**：
- 截取手机号前3位作为区号
- 提取身份证号的出生年月
- 截取长文本的摘要

### dx_replace - 字符串替换
字符串内容替换，支持正则表达式。

**参数说明**：
- `paras[0]`: 查找的字符串或正则表达式
- `paras[1]`: 替换为的字符串

**配置示例**：
```json
{
  "name": "dx_replace",
  "parameter": {
    "columnIndex": 1,
    "paras": ["@old-domain.com", "@new-domain.com"]
  }
}
```

**使用场景**：
- 替换邮箱域名
- 清理电话号码中的特殊字符
- 标准化数据格式

### dx_pad - 字符串填充
对字符串进行左填充或右填充。

**参数说明**：
- `paras[0]`: 填充方向（"left"或"right"）
- `paras[1]`: 目标长度
- `paras[2]`: 填充字符

**配置示例**：
```json
{
  "name": "dx_pad",
  "parameter": {
    "columnIndex": 1,
    "paras": ["left", "10", "0"]
  }
}
```

**使用场景**：
- 编号前补零：001, 002, 003
- 文本右侧补空格对齐
- 格式化输出固定长度字段

### dx_digest - 数据摘要
对数据进行哈希摘要计算。

**支持的算法**：
- MD5
- SHA1
- SHA256

**配置示例**：
```json
{
  "name": "dx_digest",
  "parameter": {
    "columnIndex": 1,
    "paras": ["md5"]
  }
}
```

**使用场景**：
- 密码字段哈希加密
- 生成数据指纹
- 数据脱敏处理

## 配置方式

### 单个Transformer
```json
{
  "job": {
    "content": [{
      "reader": { ... },
      "writer": { ... },
      "transformer": [{
        "name": "dx_filter",
        "parameter": {
          "columnIndex": 2,
          "paras": [">", "18"]
        }
      }]
    }]
  }
}
```

### 多个Transformer链式处理
```json
{
  "transformer": [
    {
      "name": "dx_substr",
      "parameter": {
        "columnIndex": 1,
        "paras": ["0", "10"]
      }
    },
    {
      "name": "dx_filter",
      "parameter": {
        "columnIndex": 2,
        "paras": [">", "100"]
      }
    },
    {
      "name": "dx_replace",
      "parameter": {
        "columnIndex": 3,
        "paras": ["old", "new"]
      }
    }
  ]
}
```

## 执行机制

### 处理流程
1. **记录读取**：Reader读取一条记录
2. **链式转换**：按配置顺序执行每个Transformer
3. **过滤检查**：如果任何Transformer返回null，记录被过滤掉
4. **记录写入**：通过所有Transformer的记录发送给Writer

### 性能特点
- **流式处理**：逐条记录处理，内存占用小
- **零拷贝优化**：尽可能避免不必要的数据复制
- **类型安全**：基于强类型的Record/Column系统

## 实际应用示例

### 用户数据清洗
```json
{
  "transformer": [
    {
      "name": "dx_replace",
      "parameter": {
        "columnIndex": 1,
        "paras": ["\\s+", ""]
      }
    },
    {
      "name": "dx_filter",
      "parameter": {
        "columnIndex": 2,
        "paras": ["like", "%@%.%"]
      }
    },
    {
      "name": "dx_substr",
      "parameter": {
        "columnIndex": 3,
        "paras": ["0", "11"]
      }
    }
  ]
}
```

此配置会：
1. 清理姓名字段中的所有空格
2. 过滤出有效的邮箱地址
3. 截取手机号为11位

### 订单数据标准化
```json
{
  "transformer": [
    {
      "name": "dx_pad",
      "parameter": {
        "columnIndex": 0,
        "paras": ["left", "8", "0"]
      }
    },
    {
      "name": "dx_filter",
      "parameter": {
        "columnIndex": 4,
        "paras": ["!=", "cancelled"]
      }
    },
    {
      "name": "dx_digest",
      "parameter": {
        "columnIndex": 5,
        "paras": ["md5"]
      }
    }
  ]
}
```

此配置会：
1. 订单号补零至8位
2. 过滤掉已取消的订单
3. 对客户信息进行MD5脱敏

## 注意事项

### 列索引
- columnIndex从0开始计数
- 必须对应Reader中column配置的字段顺序

### 数据类型
- Transformer会自动处理数据类型转换
- 建议在配置前了解源数据的类型

### 性能考虑
- 过多的Transformer会影响处理性能
- filter类型的Transformer应尽早配置，减少后续处理量
- 正则表达式操作相对较慢，谨慎使用

### 错误处理
- 如果转换失败，记录会被标记为错误
- 可通过job配置中的errorLimit控制错误容忍度