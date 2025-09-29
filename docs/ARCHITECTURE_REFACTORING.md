# DataX Go 架构重构说明

## 重构目标

按照Java DataX的设计理念，将Go版本重构为：
- **API包**: 纯接口定义，类似Java的common包
- **Core包**: 具体实现，类似Java的core包
- **插件**: 只需要依赖API包，降低耦合度

## 新架构结构

```
api/                    # 纯接口定义 - 类似Java common包
├── element/           # 数据元素接口
├── plugin/            # 插件接口
├── config/            # 配置接口
├── transformer/       # 转换器接口
├── engine/            # 引擎接口
├── statistics/        # 统计接口
└── logger/            # 日志接口

core/                   # 具体实现 - 类似Java core包
├── element/           # 数据元素实现
├── plugin/            # 插件基础实现
├── config/            # 配置实现
├── transformer/       # 转换器实现
├── factory/           # 工厂模式实现
├── statistics/        # 统计实现
└── logger/            # 日志实现

plugins/               # 插件实现（只依赖api包）
├── reader/
└── writer/
```

## 新架构使用示例

### 1. 插件实现（新方式）

```go
// plugins/reader/examplereader/example_reader.go
package examplereader

import (
    "github.com/longkeyy/go-datax/api/config"
    "github.com/longkeyy/go-datax/api/plugin"
    "github.com/longkeyy/go-datax/core/factory"
)

type ExampleReaderTask struct {
    config config.Configuration
}

func (r *ExampleReaderTask) Init(config config.Configuration) error {
    r.config = config
    return nil
}

func (r *ExampleReaderTask) StartRead(recordSender plugin.RecordSender) error {
    // 使用工厂创建记录和列
    factory := factory.GetGlobalFactory()
    record := factory.GetRecordFactory().CreateRecord()

    // 添加数据
    column := factory.GetColumnFactory().CreateStringColumn("example data")
    record.AddColumn(column)

    return recordSender.SendRecord(record)
}

// ... 其他方法实现

// init.go - 注册插件
func init() {
    factory := factory.GetGlobalFactory()
    registry := factory.GetPluginRegistry()

    registry.RegisterReaderTask("examplereader", &ExampleReaderTaskFactory{})
}

type ExampleReaderTaskFactory struct{}

func (f *ExampleReaderTaskFactory) CreateReaderTask() plugin.ReaderTask {
    return &ExampleReaderTask{}
}
```

### 2. 依赖关系

```
插件包 -> 只依赖 api/ 包
core包 -> 实现 api/ 包的接口
应用主程序 -> 使用 core/factory 装配所有组件
```

## 重构的好处

1. **依赖倒置**: 插件只依赖抽象接口，不依赖具体实现
2. **扩展友好**: 新插件只需实现api接口，无需了解内部实现
3. **测试友好**: 可以轻松mock接口进行单元测试
4. **维护性**: 接口和实现分离，修改实现不影响插件
5. **一致性**: 与Java DataX架构保持一致

## 迁移计划

1. ✅ 创建api包的纯接口定义
2. ✅ 创建core包的具体实现
3. ✅ 创建工厂模式管理组件创建
4. 🔄 逐步更新现有插件使用新接口
5. ⏳ 更新所有import语句
6. ⏳ 运行测试确保功能正确

## 兼容性

重构过程中将保持向后兼容，现有插件可以逐步迁移到新架构。