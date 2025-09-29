# 插件开发指南

## 当前架构

go-datax采用三层架构，完全复刻Java DataX设计：

```
core/                   # 核心引擎实现
├── engine/            # 程序入口和作业调度
├── job/               # 作业容器和生命周期管理
├── taskgroup/         # 任务组容器和并发执行
├── plugin/            # 插件接口定义
└── element/           # 数据元素抽象

common/                 # 通用组件
├── config/            # 配置管理
├── statistics/        # 监控统计
├── logger/            # 分层日志
└── plugin/            # 插件注册中心

plugins/               # 数据源插件
├── reader/            # 读取插件
└── writer/            # 写入插件
```

## 插件开发指南

### 1. 插件结构

每个插件必须包含以下文件：
- `{plugin_name}.go` - 核心实现
- `init.go` - 插件注册

### 2. 插件实现模板

```go
// Reader插件实现
type ExampleReaderTask struct {
    readerConfig *config.Configuration
}

func (r *ExampleReaderTask) Init(config *config.Configuration) error {
    r.readerConfig = config
    return nil
}

func (r *ExampleReaderTask) StartRead(recordSender plugin.RecordSender) error {
    // 读取数据并发送
    record := element.NewDefaultRecord()
    column := element.NewStringColumn("data")
    record.AddColumn(column)
    return recordSender.SendRecord(record)
}

// init.go - 插件注册
func init() {
    plugin.RegisterReader("examplereader", &ExampleReaderJobFactory{}, &ExampleReaderTaskFactory{})
}
```

## 插件开发最佳实践

### 1. 配置管理
- 使用`config.GetString()`, `config.GetInt()`等标准方法
- 实现参数验证和默认值设置
- 支持必需参数和可选参数

### 2. 错误处理
- 返回具体的错误信息
- 使用logger进行日志记录
- 实现重试机制（如适用）

### 3. 性能优化
- 使用批量操作减少网络开销
- 实现连接池管理
- 支持并发读写

### 4. Java配置兼容
- 严格遵循DataX Java配置格式
- 保持参数名称和结构一致
- 支持相同的数据类型转换