package transformer

import (
	"github.com/longkeyy/go-datax/common/element"
)

// Transformer 数据转换器接口
type Transformer interface {
	// GetTransformerName 获取转换器名称
	GetTransformerName() string

	// Evaluate 对单条记录进行转换
	// 参数：record - 输入记录，paras - 转换参数
	// 返回：转换后的记录，如果返回nil表示该记录被过滤
	Evaluate(record element.Record, paras ...interface{}) (element.Record, error)

	// SetTransformerName 设置转换器名称
	SetTransformerName(name string)
}

// TransformerInfo 转换器信息
type TransformerInfo struct {
	transformer Transformer
	isNative    bool
}

func NewTransformerInfo(transformer Transformer, isNative bool) *TransformerInfo {
	return &TransformerInfo{
		transformer: transformer,
		isNative:    isNative,
	}
}

func (ti *TransformerInfo) GetTransformer() Transformer {
	return ti.transformer
}

func (ti *TransformerInfo) IsNative() bool {
	return ti.isNative
}