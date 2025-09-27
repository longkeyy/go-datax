package transformer

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"github.com/longkeyy/go-datax/common/element"
	"strconv"
	"strings"
)

// PadTransformer 字符串填充转换器
type PadTransformer struct {
	BaseTransformer
}

func NewPadTransformer() *PadTransformer {
	pt := &PadTransformer{}
	pt.SetTransformerName("dx_pad")
	return pt
}

func (pt *PadTransformer) Evaluate(record element.Record, paras ...interface{}) (element.Record, error) {
	if len(paras) != 4 {
		return nil, fmt.Errorf("dx_pad requires exactly 4 parameters, got %d", len(paras))
	}

	// 解析参数
	columnIndex, ok := paras[0].(int)
	if !ok {
		return nil, fmt.Errorf("dx_pad: columnIndex must be int, got %T", paras[0])
	}

	padType, ok := paras[1].(string)
	if !ok {
		return nil, fmt.Errorf("dx_pad: padType must be string, got %T", paras[1])
	}

	lengthStr, ok := paras[2].(string)
	if !ok {
		return nil, fmt.Errorf("dx_pad: length must be string, got %T", paras[2])
	}

	padString, ok := paras[3].(string)
	if !ok {
		return nil, fmt.Errorf("dx_pad: padString must be string, got %T", paras[3])
	}

	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		return nil, fmt.Errorf("dx_pad: invalid length '%s': %v", lengthStr, err)
	}

	// 获取要处理的列
	if columnIndex >= record.GetColumnNumber() {
		return nil, fmt.Errorf("dx_pad: columnIndex %d out of range, record has %d columns", columnIndex, record.GetColumnNumber())
	}

	column := record.GetColumn(columnIndex)
	if column == nil {
		return record, nil // 空值跳过处理
	}

	// 获取原始字符串值
	oriValue := column.GetAsString()
	if oriValue == "" {
		return record, nil
	}

	// 执行填充
	var newValue string
	currentLen := len(oriValue)

	if currentLen >= length {
		// 如果当前长度已达到或超过目标长度，直接返回
		newValue = oriValue
	} else {
		padLen := length - currentLen
		padding := strings.Repeat(padString, (padLen/len(padString))+1)[:padLen]

		switch strings.ToLower(padType) {
		case "left", "l":
			newValue = padding + oriValue
		case "right", "r":
			newValue = oriValue + padding
		default:
			return nil, fmt.Errorf("dx_pad: unsupported padType '%s', use 'left' or 'right'", padType)
		}
	}

	// 创建新列并设置
	newColumn := element.NewStringColumn(newValue)
	record.SetColumn(columnIndex, newColumn)

	return record, nil
}

// ReplaceTransformer 字符串替换转换器
type ReplaceTransformer struct {
	BaseTransformer
}

func NewReplaceTransformer() *ReplaceTransformer {
	rt := &ReplaceTransformer{}
	rt.SetTransformerName("dx_replace")
	return rt
}

func (rt *ReplaceTransformer) Evaluate(record element.Record, paras ...interface{}) (element.Record, error) {
	if len(paras) != 3 {
		return nil, fmt.Errorf("dx_replace requires exactly 3 parameters, got %d", len(paras))
	}

	// 解析参数
	columnIndex, ok := paras[0].(int)
	if !ok {
		return nil, fmt.Errorf("dx_replace: columnIndex must be int, got %T", paras[0])
	}

	searchString, ok := paras[1].(string)
	if !ok {
		return nil, fmt.Errorf("dx_replace: searchString must be string, got %T", paras[1])
	}

	replacement, ok := paras[2].(string)
	if !ok {
		return nil, fmt.Errorf("dx_replace: replacement must be string, got %T", paras[2])
	}

	// 获取要处理的列
	if columnIndex >= record.GetColumnNumber() {
		return nil, fmt.Errorf("dx_replace: columnIndex %d out of range, record has %d columns", columnIndex, record.GetColumnNumber())
	}

	column := record.GetColumn(columnIndex)
	if column == nil {
		return record, nil // 空值跳过处理
	}

	// 获取原始字符串值
	oriValue := column.GetAsString()
	if oriValue == "" {
		return record, nil
	}

	// 执行替换
	newValue := strings.ReplaceAll(oriValue, searchString, replacement)

	// 创建新列并设置
	newColumn := element.NewStringColumn(newValue)
	record.SetColumn(columnIndex, newColumn)

	return record, nil
}

// DigestTransformer 数据摘要转换器
type DigestTransformer struct {
	BaseTransformer
}

func NewDigestTransformer() *DigestTransformer {
	dt := &DigestTransformer{}
	dt.SetTransformerName("dx_digest")
	return dt
}

func (dt *DigestTransformer) Evaluate(record element.Record, paras ...interface{}) (element.Record, error) {
	if len(paras) != 2 {
		return nil, fmt.Errorf("dx_digest requires exactly 2 parameters, got %d", len(paras))
	}

	// 解析参数
	columnIndex, ok := paras[0].(int)
	if !ok {
		return nil, fmt.Errorf("dx_digest: columnIndex must be int, got %T", paras[0])
	}

	algorithm, ok := paras[1].(string)
	if !ok {
		return nil, fmt.Errorf("dx_digest: algorithm must be string, got %T", paras[1])
	}

	// 获取要处理的列
	if columnIndex >= record.GetColumnNumber() {
		return nil, fmt.Errorf("dx_digest: columnIndex %d out of range, record has %d columns", columnIndex, record.GetColumnNumber())
	}

	column := record.GetColumn(columnIndex)
	if column == nil {
		return record, nil // 空值跳过处理
	}

	// 获取原始字符串值
	oriValue := column.GetAsString()
	if oriValue == "" {
		return record, nil
	}

	// 计算摘要
	var digest string
	switch strings.ToLower(algorithm) {
	case "md5":
		hash := md5.Sum([]byte(oriValue))
		digest = fmt.Sprintf("%x", hash)
	case "sha1":
		hash := sha1.Sum([]byte(oriValue))
		digest = fmt.Sprintf("%x", hash)
	case "sha256":
		hash := sha256.Sum256([]byte(oriValue))
		digest = fmt.Sprintf("%x", hash)
	default:
		return nil, fmt.Errorf("dx_digest: unsupported algorithm '%s', use 'md5', 'sha1', or 'sha256'", algorithm)
	}

	// 创建新列并设置
	newColumn := element.NewStringColumn(digest)
	record.SetColumn(columnIndex, newColumn)

	return record, nil
}

// GroovyTransformer Groovy脚本转换器（简化实现）
// 注意：这是一个简化的实现，实际的Groovy支持需要集成脚本引擎
type GroovyTransformer struct {
	BaseTransformer
}

func NewGroovyTransformer() *GroovyTransformer {
	gt := &GroovyTransformer{}
	gt.SetTransformerName("dx_groovy")
	return gt
}

func (gt *GroovyTransformer) Evaluate(record element.Record, paras ...interface{}) (element.Record, error) {
	if len(paras) != 2 {
		return nil, fmt.Errorf("dx_groovy requires exactly 2 parameters, got %d", len(paras))
	}

	// 解析参数
	code, ok := paras[0].(string)
	if !ok {
		return nil, fmt.Errorf("dx_groovy: code must be string, got %T", paras[0])
	}

	extraPackage, ok := paras[1].([]string)
	if !ok {
		// 尝试转换interface{}类型
		if extraInterface, ok := paras[1].([]interface{}); ok {
			extraPackage = make([]string, len(extraInterface))
			for i, v := range extraInterface {
				if s, ok := v.(string); ok {
					extraPackage[i] = s
				} else {
					return nil, fmt.Errorf("dx_groovy: extraPackage element must be string, got %T", v)
				}
			}
		} else {
			return nil, fmt.Errorf("dx_groovy: extraPackage must be []string, got %T", paras[1])
		}
	}

	// 这里是简化实现，实际应该集成Groovy脚本引擎
	// 为了保持兼容性，我们提供一个基础的脚本执行框架
	_ = code
	_ = extraPackage

	// 简化实现：记录日志并返回原记录
	fmt.Printf("dx_groovy: executing script (simplified implementation)\n")
	fmt.Printf("Code: %s\n", code)
	fmt.Printf("Extra packages: %v\n", extraPackage)

	// 在完整实现中，这里应该：
	// 1. 创建Groovy脚本引擎
	// 2. 加载额外的包
	// 3. 执行脚本代码
	// 4. 返回脚本处理后的record

	return record, nil
}

// 注册高级转换器
func init() {
	RegisterNativeTransformer(NewPadTransformer())
	RegisterNativeTransformer(NewReplaceTransformer())
	RegisterNativeTransformer(NewDigestTransformer())
	RegisterNativeTransformer(NewGroovyTransformer())
}