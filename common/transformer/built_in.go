package transformer

import (
	"fmt"
	"github.com/longkeyy/go-datax/common/element"
	"strconv"
)

// BaseTransformer 基础转换器，实现通用逻辑
type BaseTransformer struct {
	transformerName string
}

func (bt *BaseTransformer) GetTransformerName() string {
	return bt.transformerName
}

func (bt *BaseTransformer) SetTransformerName(name string) {
	bt.transformerName = name
}

// SubstrTransformer 字符串截取转换器
type SubstrTransformer struct {
	BaseTransformer
}

func NewSubstrTransformer() *SubstrTransformer {
	st := &SubstrTransformer{}
	st.SetTransformerName("dx_substr")
	return st
}

func (st *SubstrTransformer) Evaluate(record element.Record, paras ...interface{}) (element.Record, error) {
	if len(paras) != 3 {
		return nil, fmt.Errorf("dx_substr requires exactly 3 parameters, got %d", len(paras))
	}

	// 解析参数
	columnIndex, ok := paras[0].(int)
	if !ok {
		return nil, fmt.Errorf("dx_substr: columnIndex must be int, got %T", paras[0])
	}

	startIndexStr, ok := paras[1].(string)
	if !ok {
		return nil, fmt.Errorf("dx_substr: startIndex must be string, got %T", paras[1])
	}

	lengthStr, ok := paras[2].(string)
	if !ok {
		return nil, fmt.Errorf("dx_substr: length must be string, got %T", paras[2])
	}

	startIndex, err := strconv.Atoi(startIndexStr)
	if err != nil {
		return nil, fmt.Errorf("dx_substr: invalid startIndex '%s': %v", startIndexStr, err)
	}

	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		return nil, fmt.Errorf("dx_substr: invalid length '%s': %v", lengthStr, err)
	}

	// 获取要处理的列
	if columnIndex >= record.GetColumnNumber() {
		return nil, fmt.Errorf("dx_substr: columnIndex %d out of range, record has %d columns", columnIndex, record.GetColumnNumber())
	}

	column := record.GetColumn(columnIndex)
	if column == nil {
		return record, nil // 空值跳过处理
	}

	// 获取原始字符串值
	oriValue := column.GetAsString()
	if oriValue == "" {
		return record, nil // 空值时跳过处理
	}

	// 执行字符串截取
	oriLen := len(oriValue)
	if startIndex > oriLen {
		return nil, fmt.Errorf("dx_substr: startIndex(%d) out of range(%d)", startIndex, oriLen)
	}

	endIndex := startIndex + length
	if endIndex > oriLen {
		endIndex = oriLen
	}

	newValue := oriValue[startIndex:endIndex]

	// 创建新列并设置
	newColumn := element.NewStringColumn(newValue)
	record.SetColumn(columnIndex, newColumn)

	return record, nil
}

// FilterTransformer 数据过滤转换器
type FilterTransformer struct {
	BaseTransformer
}

func NewFilterTransformer() *FilterTransformer {
	ft := &FilterTransformer{}
	ft.SetTransformerName("dx_filter")
	return ft
}

func (ft *FilterTransformer) Evaluate(record element.Record, paras ...interface{}) (element.Record, error) {
	if len(paras) != 3 {
		return nil, fmt.Errorf("dx_filter requires exactly 3 parameters, got %d", len(paras))
	}

	// 解析参数
	columnIndex, ok := paras[0].(int)
	if !ok {
		return nil, fmt.Errorf("dx_filter: columnIndex must be int, got %T", paras[0])
	}

	operator, ok := paras[1].(string)
	if !ok {
		return nil, fmt.Errorf("dx_filter: operator must be string, got %T", paras[1])
	}

	value, ok := paras[2].(string)
	if !ok {
		return nil, fmt.Errorf("dx_filter: value must be string, got %T", paras[2])
	}

	if value == "" {
		return nil, fmt.Errorf("dx_filter: value parameter cannot be empty")
	}

	// 获取要处理的列
	if columnIndex >= record.GetColumnNumber() {
		return nil, fmt.Errorf("dx_filter: columnIndex %d out of range, record has %d columns", columnIndex, record.GetColumnNumber())
	}

	column := record.GetColumn(columnIndex)

	// 根据操作符执行过滤
	switch operator {
	case "=", "==":
		return ft.doEqual(record, value, column)
	case "!=":
		return ft.doNotEqual(record, value, column)
	case ">":
		return ft.doGreater(record, value, column, false)
	case ">=":
		return ft.doGreater(record, value, column, true)
	case "<":
		return ft.doLess(record, value, column, false)
	case "<=":
		return ft.doLess(record, value, column, true)
	case "like":
		return ft.doLike(record, value, column)
	case "not like":
		return ft.doNotLike(record, value, column)
	default:
		return nil, fmt.Errorf("dx_filter: unsupported operator '%s'", operator)
	}
}

func (ft *FilterTransformer) doEqual(record element.Record, value string, column element.Column) (element.Record, error) {
	if column == nil {
		// 空值只与"null"匹配
		if value == "null" {
			return nil, nil // 过滤掉
		}
		return record, nil
	}

	columnType := column.GetType()
	switch columnType {
	case element.TypeDouble:
		oriValue, err := column.GetAsDouble()
		if err != nil {
			return record, nil
		}
		targetValue, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return record, nil
		}
		if oriValue == targetValue {
			return nil, nil // 过滤掉
		}
	case element.TypeLong, element.TypeDate:
		oriValue, err := column.GetAsLong()
		if err != nil {
			return record, nil
		}
		targetValue, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return record, nil
		}
		if oriValue == targetValue {
			return nil, nil // 过滤掉
		}
	default:
		oriValue := column.GetAsString()
		if oriValue == value {
			return nil, nil // 过滤掉
		}
	}

	return record, nil
}

func (ft *FilterTransformer) doNotEqual(record element.Record, value string, column element.Column) (element.Record, error) {
	result, err := ft.doEqual(record, value, column)
	if err != nil {
		return nil, err
	}
	// 反转结果：相等时不过滤，不相等时过滤
	if result == nil {
		return record, nil
	}
	return nil, nil
}

func (ft *FilterTransformer) doGreater(record element.Record, value string, column element.Column, hasEqual bool) (element.Record, error) {
	if column == nil {
		return record, nil // 空值不参与比较
	}

	columnType := column.GetType()
	switch columnType {
	case element.TypeDouble:
		oriValue, err := column.GetAsDouble()
		if err != nil {
			return record, nil
		}
		targetValue, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return record, nil
		}
		if hasEqual {
			if oriValue >= targetValue {
				return nil, nil // 过滤掉
			}
		} else {
			if oriValue > targetValue {
				return nil, nil // 过滤掉
			}
		}
	case element.TypeLong, element.TypeDate:
		oriValue, err := column.GetAsLong()
		if err != nil {
			return record, nil
		}
		targetValue, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return record, nil
		}
		if hasEqual {
			if oriValue >= targetValue {
				return nil, nil // 过滤掉
			}
		} else {
			if oriValue > targetValue {
				return nil, nil // 过滤掉
			}
		}
	default:
		oriValue := column.GetAsString()
		if hasEqual {
			if oriValue >= value {
				return nil, nil // 过滤掉
			}
		} else {
			if oriValue > value {
				return nil, nil // 过滤掉
			}
		}
	}

	return record, nil
}

func (ft *FilterTransformer) doLess(record element.Record, value string, column element.Column, hasEqual bool) (element.Record, error) {
	if column == nil {
		return record, nil // 空值不参与比较
	}

	columnType := column.GetType()
	switch columnType {
	case element.TypeDouble:
		oriValue, err := column.GetAsDouble()
		if err != nil {
			return record, nil
		}
		targetValue, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return record, nil
		}
		if hasEqual {
			if oriValue <= targetValue {
				return nil, nil // 过滤掉
			}
		} else {
			if oriValue < targetValue {
				return nil, nil // 过滤掉
			}
		}
	case element.TypeLong, element.TypeDate:
		oriValue, err := column.GetAsLong()
		if err != nil {
			return record, nil
		}
		targetValue, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return record, nil
		}
		if hasEqual {
			if oriValue <= targetValue {
				return nil, nil // 过滤掉
			}
		} else {
			if oriValue < targetValue {
				return nil, nil // 过滤掉
			}
		}
	default:
		oriValue := column.GetAsString()
		if hasEqual {
			if oriValue <= value {
				return nil, nil // 过滤掉
			}
		} else {
			if oriValue < value {
				return nil, nil // 过滤掉
			}
		}
	}

	return record, nil
}

func (ft *FilterTransformer) doLike(record element.Record, pattern string, column element.Column) (element.Record, error) {
	if column == nil {
		return record, nil
	}

	oriValue := column.GetAsString()

	// 简化的like实现，这里使用Go的字符串包含匹配
	// 在完整实现中应该支持SQL like语法（%和_通配符）
	if len(oriValue) > 0 && len(pattern) > 0 {
		// 这里应该实现完整的SQL LIKE语法，暂时用简单的包含匹配
		if oriValue == pattern {
			return nil, nil // 过滤掉
		}
	}

	return record, nil
}

func (ft *FilterTransformer) doNotLike(record element.Record, pattern string, column element.Column) (element.Record, error) {
	result, err := ft.doLike(record, pattern, column)
	if err != nil {
		return nil, err
	}
	// 反转结果
	if result == nil {
		return record, nil
	}
	return nil, nil
}

// 注册内置转换器
func init() {
	RegisterNativeTransformer(NewSubstrTransformer())
	RegisterNativeTransformer(NewFilterTransformer())
}