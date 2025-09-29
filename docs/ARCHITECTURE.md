# go-datax 架构设计

go-datax采用三层架构设计，完全基于Go语言实现，保持与DataX Java版的功能兼容性。

## 1. 总体架构

```
┌─────────────────────────────────────────┐
│             Engine Layer                │
│         (程序入口和配置管理)            │
├─────────────────────────────────────────┤
│           Container Layer               │
│  ┌─────────────┐  ┌──────────────────┐  │
│  │JobContainer │  │TaskGroupContainer│  │
│  │    (作业)   │  │    (任务组)      │  │
│  └─────────────┘  └──────────────────┘  │
├─────────────────────────────────────────┤
│             Plugin Layer                │
│  ┌───────────┐ ┌────────┐ ┌───────────┐ │
│  │  Reader   │ │Channel │ │  Writer   │ │
│  │ Plugins   │ │        │ │ Plugins   │ │
│  └───────────┘ └────────┘ └───────────┘ │
└─────────────────────────────────────────┘
```

## 2. 核心模块架构

### Engine Layer (引擎层)
**位置**: `cmd/datax/main.go` + `core/engine/`

```go
// main.go - 程序入口
func main() {
    // 自动注册所有插件
    engine.Main(Version)
}

// Engine核心功能
type Engine struct {
    configuration *config.Configuration
}

func (e *Engine) Start() error {
    // 1. 解析命令行参数
    // 2. 加载配置文件
    // 3. 创建JobContainer
    // 4. 启动作业执行
}
```

**职责**:
- 命令行参数解析 (`-job`, `-version`)
- JSON配置文件加载和验证
- 全局错误处理和退出码管理
- JobContainer生命周期管理

### Container Layer (容器层)

#### JobContainer (作业容器)
**位置**: `core/job/`

```go
type JobContainer struct {
    configuration  *config.Configuration
    taskGroupMap   map[int]*taskgroup.TaskGroupContainer
    pluginRegistry *plugin.Registry
}

func (jc *JobContainer) Start() error {
    // 作业生命周期: init → prepare → split → schedule → destroy
    if err := jc.init(); err != nil { return err }
    if err := jc.prepare(); err != nil { return err }
    if err := jc.split(); err != nil { return err }
    return jc.schedule()
}
```

**核心功能**:
1. **插件初始化**: 加载Reader/Writer插件
2. **任务拆分**: 将单个作业拆分为多个并行任务
3. **调度管理**: 创建和管理TaskGroupContainer
4. **资源协调**: 统一管理配置、统计、日志

#### TaskGroupContainer (任务组容器)
**位置**: `core/taskgroup/`

```go
type TaskGroupContainer struct {
    taskGroupId int
    channel     chan element.Record
    reader      plugin.ReaderTask
    writer      plugin.WriterTask
    statistics  *statistics.Container
}

func (tgc *TaskGroupContainer) Start() error {
    // 创建Goroutine并发执行Reader和Writer
    go tgc.startReader()
    go tgc.startWriter()
    return tgc.wait()
}
```

**Go语言特性**:
- **Goroutine**: 轻量级协程替代Java线程
- **Channel**: 原生通道替代Java BlockingQueue
- **Context**: 优雅的超时和取消控制

### Plugin Layer (插件层)

#### 插件接口设计
**位置**: `core/plugin/`

```go
// 核心插件接口
type ReaderTask interface {
    Init(config *config.Configuration) error
    Prepare(config *config.Configuration) error
    StartRead(recordSender RecordSender) error
    Post(config *config.Configuration) error
    Destroy(config *config.Configuration) error
}

type WriterTask interface {
    Init(config *config.Configuration) error
    Prepare(config *config.Configuration) error
    StartWrite(recordReceiver RecordReceiver) error
    Post(config *config.Configuration) error
    Destroy(config *config.Configuration) error
}
```

#### 工厂注册模式
```go
// 插件自动注册机制
func init() {
    plugin.RegisterReader("postgresqlreader",
        &PostgresqlReaderJobFactory{},
        &PostgresqlReaderTaskFactory{})
}

type PluginRegistry struct {
    readerFactories map[string]ReaderTaskFactory
    writerFactories map[string]WriterTaskFactory
    mu              sync.RWMutex
}
```

## 3. 数据流架构

### 数据传输模型
```
┌─────────────┐    Channel    ┌──────────────┐
│ Reader Task │ ─────────────→│ Writer Task  │
│             │   (Goroutine  │              │
│   Goroutine │    Safe)      │  Goroutine   │
└─────────────┘               └──────────────┘
       ↓                             ↑
  RecordSender                RecordReceiver
       ↓                             ↑
┌─────────────────────────────────────────────┐
│            element.Record                   │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐    │
│  │ Column1  │ │ Column2  │ │ Column3  │    │
│  └──────────┘ └──────────┘ └──────────┘    │
└─────────────────────────────────────────────┘
```

### Record/Column 类型系统
**位置**: `core/element/`

```go
// Record 接口
type Record interface {
    AddColumn(column Column)
    SetColumn(i int, column Column)
    GetColumn(i int) Column
    GetColumnNumber() int
    GetByteSize() int
    GetMemorySize() int
}

// Column 类型
type Column interface {
    GetAsString() string
    GetAsLong() (int64, error)
    GetAsDouble() (float64, error)
    GetAsBytes() []byte
    GetType() ColumnType
}
```

**支持的数据类型**:
```go
const (
    STRING ColumnType = iota
    LONG
    DOUBLE
    DATE
    BOOL
    BYTES
    NULL
)
```

## 4. 配置管理架构

### Configuration 系统
**位置**: `common/config/`

```go
type Configuration struct {
    root map[string]interface{}
    mu   sync.RWMutex
}

// 核心方法
func (c *Configuration) GetString(path string) string
func (c *Configuration) GetInt(path string) int
func (c *Configuration) GetBool(path string) bool
func (c *Configuration) GetList(path string) []*Configuration
func (c *Configuration) Set(path string, value interface{})
```

### 配置层次结构
```json
{
  "job": {
    "setting": {
      "speed": {"channel": 4},
      "errorLimit": {"record": 100}
    },
    "content": [{
      "reader": {
        "name": "postgresqlreader",
        "parameter": {...}
      },
      "writer": {
        "name": "mysqlwriter",
        "parameter": {...}
      },
      "transformer": [...]
    }]
  }
}
```

## 5. 监控统计架构

### Statistics 系统
**位置**: `common/statistics/`

```go
type Container struct {
    totalCosts    time.Duration
    byteSpeed     int64
    recordSpeed   int64
    totalReadRecords  int64
    totalErrorRecords int64
    mu            sync.RWMutex
}

func (s *Container) Report() *Report {
    // 生成实时统计报告
}
```

### 三层日志架构
**位置**: `common/logger/`

```go
// 基于zap的结构化日志
var (
    AppLogger       *zap.Logger    // 应用级日志
    ComponentLogger *zap.Logger    // 组件级日志
    TaskLogger      *zap.Logger    // 任务级日志
)

// 使用示例
logger.App().Info("Job started", zap.String("jobId", id))
logger.Component().Error("Plugin load failed", zap.Error(err))
logger.Task(taskId).Debug("Record processed", zap.Int64("count", count))
```

## 6. 插件生态架构

### 插件目录结构
```
plugins/
├── reader/
│   ├── postgresqlreader/
│   │   ├── postgresql_reader.go
│   │   └── init.go
│   ├── mysqlreader/
│   └── jsonfilereader/
└── writer/
    ├── postgresqlwriter/
    ├── mysqlwriter/
    └── jsonfilewriter/
```

### 插件实现模板
```go
package postgresqlreader

import (
    "github.com/longkeyy/go-datax/core/plugin"
    "github.com/longkeyy/go-datax/common/config"
)

// Task实现
type PostgresqlReaderTask struct {
    readerConfig *config.Configuration
    db           *sql.DB
}

func (p *PostgresqlReaderTask) Init(config *config.Configuration) error {
    // 初始化数据库连接
}

func (p *PostgresqlReaderTask) StartRead(recordSender plugin.RecordSender) error {
    // 查询数据并发送Record
}

// 工厂注册
func init() {
    plugin.RegisterReader("postgresqlreader",
        &PostgresqlReaderJobFactory{},
        &PostgresqlReaderTaskFactory{})
}
```

## 7. Go语言优化特性

### 并发模型
- **Goroutine池**: 轻量级协程管理
- **Channel通信**: CSP模型数据传输
- **Context控制**: 优雅的取消和超时

### 内存管理
- **零拷贝优化**: 避免不必要的数据复制
- **对象池**: Record/Column对象复用
- **GC友好**: 减少堆分配，降低GC压力

### 类型安全
- **强类型系统**: 编译期类型检查
- **Interface抽象**: 灵活的插件系统
- **错误处理**: 显式错误返回和处理

### 部署优势
- **单二进制**: 无外部依赖
- **跨平台**: 支持多操作系统和架构
- **小体积**: 编译后约20MB

## 8. 扩展性设计

### 新增Reader插件
1. 实现ReaderTask接口
2. 创建工厂类
3. 在init()中注册
4. 在main.go中添加import

### 新增Writer插件
1. 实现WriterTask接口
2. 创建工厂类
3. 在init()中注册
4. 在main.go中添加import

### 新增Transformer
1. 实现Transformer接口
2. 注册到TransformerRegistry
3. 支持配置驱动的链式处理

## 9. 与Java版本对比

| 特性 | Java版本 | Go版本 |
|------|----------|--------|
| **运行时** | JVM | 原生二进制 |
| **并发模型** | Thread | Goroutine |
| **通道通信** | BlockingQueue | Channel |
| **内存管理** | JVM GC | Go GC |
| **部署** | 需要JRE | 单二进制文件 |
| **启动时间** | 较慢 | 毫秒级 |
| **资源占用** | 较高 | 较低 |
| **splitPk** | 仅数字 | 数字+字符串+日期 |

go-datax在保持100%配置兼容的基础上，充分利用了Go语言的特性优势，提供了更高效、更轻量的数据同步解决方案。