# 快速开始指南

## 安装 go-datax

### 方式1: 下载预编译二进制文件

从GitHub Releases下载对应平台的二进制文件：

**Release地址**: https://github.com/longkeyy/go-datax/releases/latest

```bash
# Linux x86_64
wget https://github.com/longkeyy/go-datax/releases/latest/download/datax-linux-amd64
chmod +x datax-linux-amd64
sudo mv datax-linux-amd64 /usr/local/bin/datax

# Linux arm64
wget https://github.com/longkeyy/go-datax/releases/latest/download/datax-linux-arm64
chmod +x datax-linux-arm64
sudo mv datax-linux-arm64 /usr/local/bin/datax

# macOS Intel
wget https://github.com/longkeyy/go-datax/releases/latest/download/datax-darwin-amd64
chmod +x datax-darwin-amd64
sudo mv datax-darwin-amd64 /usr/local/bin/datax

# macOS Apple Silicon
wget https://github.com/longkeyy/go-datax/releases/latest/download/datax-darwin-arm64
chmod +x datax-darwin-arm64
sudo mv datax-darwin-arm64 /usr/local/bin/datax

# Windows x64 (PowerShell)
Invoke-WebRequest -Uri "https://github.com/longkeyy/go-datax/releases/latest/download/datax-windows-amd64.exe" -OutFile "datax.exe"
```

### 方式2: Docker 镜像

```bash
# 拉取镜像
docker pull ghcr.io/longkeyy/go-datax:latest

# 运行容器
docker run --rm ghcr.io/longkeyy/go-datax:latest --help
```

### 方式3: 源码编译

```bash
# 克隆仓库
git clone https://github.com/longkeyy/go-datax.git
cd go-datax

# 编译
make build

# 运行
./dist/datax --help
```

## 验证安装

```bash
datax --help
```

应该看到类似输出：
```
Usage: datax [options]
Options:
  -job string
        Job config file path
  -version
        Show version information
```

## 第一个数据同步任务

### 1. 准备测试数据

创建源数据库表：
```sql
-- PostgreSQL 源表
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO users (name, email) VALUES
('Alice', 'alice@example.com'),
('Bob', 'bob@example.com'),
('Charlie', 'charlie@example.com');
```

创建目标数据库表：
```sql
-- MySQL 目标表
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at DATETIME
);
```

### 2. 创建配置文件

创建 `postgres_to_mysql.json`：

```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 2
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0.02
      }
    },
    "content": [{
      "reader": {
        "name": "postgresqlreader",
        "parameter": {
          "username": "postgres",
          "password": "password",
          "connection": [{
            "jdbcUrl": ["jdbc:postgresql://localhost:5432/testdb"],
            "table": ["users"]
          }],
          "column": ["id", "name", "email", "created_at"],
          "splitPk": "id"
        }
      },
      "writer": {
        "name": "mysqlwriter",
        "parameter": {
          "username": "root",
          "password": "password",
          "connection": [{
            "jdbcUrl": "jdbc:mysql://localhost:3306/testdb?useUnicode=true&characterEncoding=utf8",
            "table": ["users"]
          }],
          "column": ["id", "name", "email", "created_at"],
          "writeMode": "insert"
        }
      }
    }]
  }
}
```

### 3. 执行同步任务

```bash
datax -job postgres_to_mysql.json
```

成功运行后，你会看到类似输出：
```
2024-01-15 10:30:00 [INFO] - Job started
2024-01-15 10:30:01 [INFO] - Reader plugin: postgresqlreader
2024-01-15 10:30:01 [INFO] - Writer plugin: mysqlwriter
2024-01-15 10:30:02 [INFO] - Job finished successfully
2024-01-15 10:30:02 [INFO] - Total records: 3
2024-01-15 10:30:02 [INFO] - Total time: 2.1s
```

## 常用配置示例

### 增量同步

基于时间戳的增量同步：
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

### 性能优化

高并发和批量处理：
```json
{
  "job": {
    "setting": {
      "speed": {
        "channel": 8,
        "record": 100000,
        "byte": 10485760
      }
    }
  },
  "writer": {
    "parameter": {
      "batchSize": 2048
    }
  }
}
```

### JSON文件处理

读取JSON文件到数据库：
```json
{
  "reader": {
    "name": "jsonfilereader",
    "parameter": {
      "path": ["/data/users.json"],
      "encoding": "UTF-8"
    }
  },
  "writer": {
    "name": "postgresqlwriter",
    "parameter": {
      "connection": [{
        "jdbcUrl": "jdbc:postgresql://localhost:5432/testdb",
        "table": ["users"]
      }],
      "column": ["id", "name", "email"]
    }
  }
}
```

## 使用Docker运行

### 本地文件同步
```bash
# 将配置文件挂载到容器
docker run --rm \
  -v $(pwd)/config.json:/config.json \
  ghcr.io/longkeyy/go-datax:latest \
  -job /config.json
```

### 网络数据库同步
```bash
# 使用host网络模式访问本地数据库
docker run --rm --network host \
  -v $(pwd)/config.json:/config.json \
  ghcr.io/longkeyy/go-datax:latest \
  -job /config.json
```

## 故障排除

### 常见错误

**1. 连接数据库失败**
```
Error: failed to connect to database
```
解决：检查数据库连接参数、网络连通性、用户权限

**2. 表不存在**
```
Error: table "users" doesn't exist
```
解决：确认表名拼写正确，检查数据库schema

**3. 权限不足**
```
Error: permission denied
```
解决：确保数据库用户有相应的读写权限

### 调试技巧

**1. 启用详细日志**
```bash
datax -job config.json --log-level debug
```

**2. 测试连接**
先用简单的SELECT 1测试数据库连接：
```json
{
  "reader": {
    "parameter": {
      "querySql": ["SELECT 1 as test"]
    }
  }
}
```

**3. 小批量测试**
先同步少量数据进行测试：
```json
{
  "reader": {
    "parameter": {
      "where": "id <= 10"
    }
  }
}
```

## 下一步

- 查看 [架构设计文档](DataX-Architecture-Design.md) 了解工作原理
- 查看 [同步场景分析](DataX-Sync-Scenarios-Analysis.md) 了解更多使用场景
- 查看 [插件文档](plugins/) 了解具体插件配置
- 参与 [GitHub项目](https://github.com/longkeyy/go-datax) 贡献代码