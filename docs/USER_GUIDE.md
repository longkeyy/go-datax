# 数据同步场景

go-datax支持多种数据同步场景，完全兼容DataX Java版配置格式。

## 支持的同步模式

### 全量同步
同步整个表或数据集的所有数据。

**配置特点**：
- 无WHERE条件限制
- 适合数据迁移和初始化场景
- 支持splitPk并行处理

**配置示例**：
```json
{
  "reader": {
    "parameter": {
      "connection": [{
        "jdbcUrl": ["jdbc:postgresql://localhost:5432/sourcedb"],
        "table": ["users"]
      }],
      "column": ["*"],
      "splitPk": "id"
    }
  }
}
```

### 增量同步
只同步新增或变更的数据，支持多种增量策略。

#### 时间戳增量
基于时间字段进行增量同步：
```json
{
  "reader": {
    "parameter": {
      "where": "updated_at > '2024-01-01 00:00:00'",
      "splitPk": "id"
    }
  }
}
```

#### 自增ID增量
基于自增主键进行增量同步：
```json
{
  "reader": {
    "parameter": {
      "where": "id > ${lastMaxId}",
      "splitPk": "id"
    }
  }
}
```

#### 自定义SQL增量
使用复杂查询条件：
```json
{
  "reader": {
    "parameter": {
      "querySql": [
        "SELECT * FROM orders WHERE status = 'completed' AND create_time >= CURRENT_DATE - INTERVAL '7 days'"
      ]
    }
  }
}
```

### 异构数据同步
支持不同类型数据源之间的数据同步。

**关系数据库互转**：
```json
{
  "reader": {"name": "postgresqlreader"},
  "writer": {"name": "mysqlwriter"}
}
```

**关系数据库到大数据**：
```json
{
  "reader": {"name": "mysqlreader"},
  "writer": {"name": "hdfswriter"}
}
```

**文件到数据库**：
```json
{
  "reader": {"name": "txtfilereader"},
  "writer": {"name": "clickhousewriter"}
}
```

## 数据源支持矩阵

### 关系数据库
- ✅ **PostgreSQL**: 完整支持，包括分区表、复杂类型
- ✅ **MySQL**: 完整支持，包括主从分离、分库分表
- ✅ **Oracle**: 基于纯Go驱动，无需客户端
- ✅ **SQL Server**: 支持Windows认证和SQL认证
- ✅ **SQLite**: 轻量级数据库，支持文件访问
- ✅ **OceanBase**: MySQL协议兼容，支持租户
- ✅ **GaussDB**: PostgreSQL协议兼容
- ✅ **Sybase**: 基于TDS协议纯Go驱动

### 大数据存储
- ✅ **ClickHouse**: 列存数据库，高性能OLAP
- ✅ **StarRocks**: 向量化MPP数据库
- ✅ **Apache Doris**: 实时分析型数据库
- ✅ **HDFS**: Hadoop分布式文件系统
- ✅ **TDengine**: 时序数据库，IoT场景

### NoSQL数据库
- ✅ **MongoDB**: 文档数据库
- ✅ **Cassandra**: 分布式NoSQL
- ✅ **Neo4j**: 图数据库
- ✅ **ElasticSearch**: 搜索引擎

### 文件格式
- ✅ **TXT文件**: CSV、TSV等分隔符文件
- ✅ **JSON文件**: 标准JSON和JSONL格式
- ✅ **FTP/SFTP**: 远程文件传输

### 云存储
- ✅ **Databend**: 云原生数据仓库

## 性能优化功能

### 并发控制
通过channel参数控制并发度：
```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 4,           // 并发通道数
        "record": 100000,       // 记录数限制/秒
        "byte": 10485760        // 字节数限制/秒
      }
    }
  }
}
```

### 分片优化
支持多种splitPk类型（**go-datax增强功能**）：
```json
{
  "reader": {
    "parameter": {
      "splitPk": "id"              // 数字类型（兼容Java版）
    }
  }
}
```
```json
{
  "reader": {
    "parameter": {
      "splitPk": "username"        // 字符串类型（Go版增强）
    }
  }
}
```
```json
{
  "reader": {
    "parameter": {
      "splitPk": "created_at"      // 日期类型（Go版增强）
    }
  }
}
```

### 批量写入优化
```json
{
  "writer": {
    "parameter": {
      "batchSize": 2048,          // 批量大小
      "fetchSize": 1024           // 读取批次
    }
  }
}
```

### 容错机制
```json
{
  "job": {
    "setting": {
      "errorLimit": {
        "record": 100,            // 错误记录数上限
        "percentage": 0.05        // 错误百分比上限
      }
    }
  }
}
```

## 实际应用场景

### 数据仓库ETL
```json
{
  "job": {
    "content": [{
      "reader": {
        "name": "mysqlreader",
        "parameter": {
          "where": "DATE(created_at) = CURDATE() - INTERVAL 1 DAY",
          "splitPk": "id"
        }
      },
      "transformer": [{
        "name": "dx_filter",
        "parameter": {
          "columnIndex": 3,
          "paras": [">", "0"]
        }
      }],
      "writer": {
        "name": "clickhousewriter",
        "parameter": {
          "batchSize": 10000
        }
      }
    }]
  }
}
```

### 数据库迁移
```json
{
  "job": {
    "setting": {
      "speed": {"channel": 8}
    },
    "content": [{
      "reader": {
        "name": "oraclereader",
        "parameter": {
          "splitPk": "id",
          "fetchSize": 2048
        }
      },
      "writer": {
        "name": "postgresqlwriter",
        "parameter": {
          "preSql": ["TRUNCATE TABLE target_table"],
          "batchSize": 1024
        }
      }
    }]
  }
}
```

### 实时数据采集
```json
{
  "job": {
    "content": [{
      "reader": {
        "name": "tdenginereader",
        "parameter": {
          "where": "ts > now - 1h"
        }
      },
      "writer": {
        "name": "elasticsearchwriter",
        "parameter": {
          "batchSize": 500
        }
      }
    }]
  }
}
```

### 日志文件处理
```json
{
  "job": {
    "content": [{
      "reader": {
        "name": "txtfilereader",
        "parameter": {
          "path": ["/var/log/access.log"],
          "fieldDelimiter": " "
        }
      },
      "transformer": [{
        "name": "dx_filter",
        "parameter": {
          "columnIndex": 8,
          "paras": [">=", "400"]
        }
      }],
      "writer": {
        "name": "postgresqlwriter"
      }
    }]
  }
}
```

## 限制说明

### 实时同步限制
go-datax基于批处理架构，不支持真正的实时同步。

**替代方案**：
1. **准实时同步**: 定时任务 + 增量同步
2. **CDC工具**: 使用Canal、Maxwell等专门的CDC工具
3. **消息队列**: Kafka + go-datax组合

### 数据转换限制
- 不支持复杂聚合操作（GROUP BY、SUM等）
- 不支持跨记录计算
- 不支持JOIN等多表关联操作

**推荐做法**: 复杂转换在目标端进行，或使用专门的ETL工具

## 最佳实践

### 性能优化
1. **合理设置并发数**: 根据源端和目标端性能调整channel数
2. **选择合适的splitPk**: 优先选择有索引的数字类型字段
3. **批量大小调优**: 根据网络和内存情况调整batchSize
4. **避免全表扫描**: 尽量使用WHERE条件和索引

### 数据质量
1. **设置容错阈值**: 通过errorLimit控制数据质量
2. **使用Transformer**: 在传输过程中清洗和转换数据
3. **分步骤验证**: 先小批量测试，再全量同步

### 运维监控
1. **日志记录**: 保留同步日志用于问题排查
2. **监控指标**: 关注传输速度、错误率等关键指标
3. **定时检查**: 定期校验数据一致性