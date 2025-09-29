package element

import (
	"fmt"
	"time"
)

// ColumnType 定义列类型
type ColumnType int

const (
	TypeNull ColumnType = iota
	TypeLong
	TypeDouble
	TypeString
	TypeDate
	TypeBool
	TypeBytes
)

// Column 接口定义列的基本操作 - 纯接口定义，无实现
type Column interface {
	GetType() ColumnType
	GetRawData() interface{}
	GetAsString() string
	GetAsLong() (int64, error)
	GetAsDouble() (float64, error)
	GetAsDate() (time.Time, error)
	GetAsBool() (bool, error)
	GetAsBytes() ([]byte, error)
	IsNull() bool
	GetByteSize() int
}

// ColumnFactory 列工厂接口 - 创建各种类型列的抽象工厂
type ColumnFactory interface {
	CreateStringColumn(value string) Column
	CreateLongColumn(value int64) Column
	CreateDoubleColumn(value float64) Column
	CreateDateColumn(value time.Time) Column
	CreateBoolColumn(value bool) Column
	CreateBytesColumn(value []byte) Column
	CreateNullColumn(columnType ColumnType) Column
}

// DefaultColumnFactory 默认列工厂实现
type DefaultColumnFactory struct{}

func NewColumnFactory() ColumnFactory {
	return &DefaultColumnFactory{}
}

func (f *DefaultColumnFactory) CreateStringColumn(value string) Column {
	return NewStringColumn(value)
}

func (f *DefaultColumnFactory) CreateLongColumn(value int64) Column {
	return NewLongColumn(value)
}

func (f *DefaultColumnFactory) CreateDoubleColumn(value float64) Column {
	return NewDoubleColumn(value)
}

func (f *DefaultColumnFactory) CreateDateColumn(value time.Time) Column {
	return NewDateColumn(value)
}

func (f *DefaultColumnFactory) CreateBoolColumn(value bool) Column {
	return NewBoolColumn(value)
}

func (f *DefaultColumnFactory) CreateBytesColumn(value []byte) Column {
	return NewBytesColumn(value)
}

func (f *DefaultColumnFactory) CreateNullColumn(columnType ColumnType) Column {
	switch columnType {
	case TypeLong:
		return NewNullLongColumn()
	case TypeDouble:
		return NewNullDoubleColumn()
	case TypeDate:
		return NewNullDateColumn()
	case TypeBool:
		return NewNullBoolColumn()
	case TypeBytes:
		return NewNullBytesColumn()
	default:
		return NewStringColumn("")
	}
}

// BaseColumn 基础列实现
type BaseColumn struct {
	value    interface{}
	colType  ColumnType
	isNull   bool
	byteSize int
}

func (c *BaseColumn) GetType() ColumnType {
	return c.colType
}

func (c *BaseColumn) GetRawData() interface{} {
	if c.isNull {
		return nil
	}
	return c.value
}

func (c *BaseColumn) IsNull() bool {
	return c.isNull
}

func (c *BaseColumn) GetByteSize() int {
	return c.byteSize
}

// StringColumn 字符串列
type StringColumn struct {
	BaseColumn
}

func NewStringColumn(value string) *StringColumn {
	col := &StringColumn{}
	if value == "" {
		col.isNull = true
		col.byteSize = 0
	} else {
		col.value = value
		col.byteSize = len(value)
	}
	col.colType = TypeString
	return col
}

func (c *StringColumn) GetAsString() string {
	if c.isNull {
		return ""
	}
	return c.value.(string)
}

func (c *StringColumn) GetAsLong() (int64, error) {
	if c.isNull {
		return 0, fmt.Errorf("null value cannot be converted to long")
	}
	return 0, fmt.Errorf("string cannot be converted to long")
}

func (c *StringColumn) GetAsDouble() (float64, error) {
	if c.isNull {
		return 0, fmt.Errorf("null value cannot be converted to double")
	}
	return 0, fmt.Errorf("string cannot be converted to double")
}

func (c *StringColumn) GetAsDate() (time.Time, error) {
	if c.isNull {
		return time.Time{}, fmt.Errorf("null value cannot be converted to date")
	}
	return time.Time{}, fmt.Errorf("string cannot be converted to date")
}

func (c *StringColumn) GetAsBool() (bool, error) {
	if c.isNull {
		return false, fmt.Errorf("null value cannot be converted to bool")
	}
	return false, fmt.Errorf("string cannot be converted to bool")
}

func (c *StringColumn) GetAsBytes() ([]byte, error) {
	if c.isNull {
		return nil, fmt.Errorf("null value cannot be converted to bytes")
	}
	return []byte(c.value.(string)), nil
}

// LongColumn 长整型列
type LongColumn struct {
	BaseColumn
}

func NewLongColumn(value int64) *LongColumn {
	col := &LongColumn{}
	col.value = value
	col.colType = TypeLong
	col.byteSize = 8
	return col
}

func NewNullLongColumn() *LongColumn {
	col := &LongColumn{}
	col.isNull = true
	col.colType = TypeLong
	col.byteSize = 0
	return col
}

func (c *LongColumn) GetAsString() string {
	if c.isNull {
		return ""
	}
	return fmt.Sprintf("%d", c.value.(int64))
}

func (c *LongColumn) GetAsLong() (int64, error) {
	if c.isNull {
		return 0, fmt.Errorf("null value cannot be converted to long")
	}
	return c.value.(int64), nil
}

func (c *LongColumn) GetAsDouble() (float64, error) {
	if c.isNull {
		return 0, fmt.Errorf("null value cannot be converted to double")
	}
	return float64(c.value.(int64)), nil
}

func (c *LongColumn) GetAsDate() (time.Time, error) {
	if c.isNull {
		return time.Time{}, fmt.Errorf("null value cannot be converted to date")
	}
	return time.Time{}, fmt.Errorf("long cannot be converted to date")
}

func (c *LongColumn) GetAsBool() (bool, error) {
	if c.isNull {
		return false, fmt.Errorf("null value cannot be converted to bool")
	}
	return c.value.(int64) != 0, nil
}

func (c *LongColumn) GetAsBytes() ([]byte, error) {
	if c.isNull {
		return nil, fmt.Errorf("null value cannot be converted to bytes")
	}
	return []byte(c.GetAsString()), nil
}

// DateColumn 日期列
type DateColumn struct {
	BaseColumn
}

func NewDateColumn(value time.Time) *DateColumn {
	col := &DateColumn{}
	col.value = value
	col.colType = TypeDate
	col.byteSize = 8
	return col
}

func NewNullDateColumn() *DateColumn {
	col := &DateColumn{}
	col.isNull = true
	col.colType = TypeDate
	col.byteSize = 0
	return col
}

func (c *DateColumn) GetAsString() string {
	if c.isNull {
		return ""
	}
	return c.value.(time.Time).Format("2006-01-02 15:04:05")
}

func (c *DateColumn) GetAsLong() (int64, error) {
	if c.isNull {
		return 0, fmt.Errorf("null value cannot be converted to long")
	}
	return c.value.(time.Time).Unix(), nil
}

func (c *DateColumn) GetAsDouble() (float64, error) {
	if c.isNull {
		return 0, fmt.Errorf("null value cannot be converted to double")
	}
	return float64(c.value.(time.Time).Unix()), nil
}

func (c *DateColumn) GetAsDate() (time.Time, error) {
	if c.isNull {
		return time.Time{}, fmt.Errorf("null value cannot be converted to date")
	}
	return c.value.(time.Time), nil
}

func (c *DateColumn) GetAsBool() (bool, error) {
	if c.isNull {
		return false, fmt.Errorf("null value cannot be converted to bool")
	}
	return false, fmt.Errorf("date cannot be converted to bool")
}

func (c *DateColumn) GetAsBytes() ([]byte, error) {
	if c.isNull {
		return nil, fmt.Errorf("null value cannot be converted to bytes")
	}
	return []byte(c.GetAsString()), nil
}

// DoubleColumn 浮点数列
type DoubleColumn struct {
	BaseColumn
}

func NewDoubleColumn(value float64) *DoubleColumn {
	col := &DoubleColumn{}
	col.value = value
	col.colType = TypeDouble
	col.byteSize = 8
	return col
}

func NewNullDoubleColumn() *DoubleColumn {
	col := &DoubleColumn{}
	col.isNull = true
	col.colType = TypeDouble
	col.byteSize = 0
	return col
}

func (c *DoubleColumn) GetAsString() string {
	if c.isNull {
		return ""
	}
	return fmt.Sprintf("%f", c.value.(float64))
}

func (c *DoubleColumn) GetAsLong() (int64, error) {
	if c.isNull {
		return 0, fmt.Errorf("null value cannot be converted to long")
	}
	return int64(c.value.(float64)), nil
}

func (c *DoubleColumn) GetAsDouble() (float64, error) {
	if c.isNull {
		return 0, fmt.Errorf("null value cannot be converted to double")
	}
	return c.value.(float64), nil
}

func (c *DoubleColumn) GetAsDate() (time.Time, error) {
	if c.isNull {
		return time.Time{}, fmt.Errorf("null value cannot be converted to date")
	}
	return time.Time{}, fmt.Errorf("double cannot be converted to date")
}

func (c *DoubleColumn) GetAsBool() (bool, error) {
	if c.isNull {
		return false, fmt.Errorf("null value cannot be converted to bool")
	}
	return c.value.(float64) != 0, nil
}

func (c *DoubleColumn) GetAsBytes() ([]byte, error) {
	if c.isNull {
		return nil, fmt.Errorf("null value cannot be converted to bytes")
	}
	return []byte(c.GetAsString()), nil
}

// BoolColumn 布尔列
type BoolColumn struct {
	BaseColumn
}

func NewBoolColumn(value bool) *BoolColumn {
	col := &BoolColumn{}
	col.value = value
	col.colType = TypeBool
	col.byteSize = 1
	return col
}

func NewNullBoolColumn() *BoolColumn {
	col := &BoolColumn{}
	col.isNull = true
	col.colType = TypeBool
	col.byteSize = 0
	return col
}

func (c *BoolColumn) GetAsString() string {
	if c.isNull {
		return ""
	}
	if c.value.(bool) {
		return "true"
	}
	return "false"
}

func (c *BoolColumn) GetAsLong() (int64, error) {
	if c.isNull {
		return 0, fmt.Errorf("null value cannot be converted to long")
	}
	if c.value.(bool) {
		return 1, nil
	}
	return 0, nil
}

func (c *BoolColumn) GetAsDouble() (float64, error) {
	if c.isNull {
		return 0, fmt.Errorf("null value cannot be converted to double")
	}
	if c.value.(bool) {
		return 1.0, nil
	}
	return 0.0, nil
}

func (c *BoolColumn) GetAsDate() (time.Time, error) {
	if c.isNull {
		return time.Time{}, fmt.Errorf("null value cannot be converted to date")
	}
	return time.Time{}, fmt.Errorf("bool cannot be converted to date")
}

func (c *BoolColumn) GetAsBool() (bool, error) {
	if c.isNull {
		return false, fmt.Errorf("null value cannot be converted to bool")
	}
	return c.value.(bool), nil
}

func (c *BoolColumn) GetAsBytes() ([]byte, error) {
	if c.isNull {
		return nil, fmt.Errorf("null value cannot be converted to bytes")
	}
	return []byte(c.GetAsString()), nil
}

// BytesColumn 字节列
type BytesColumn struct {
	BaseColumn
}

func NewBytesColumn(value []byte) *BytesColumn {
	col := &BytesColumn{}
	if value == nil {
		col.isNull = true
		col.byteSize = 0
	} else {
		col.value = value
		col.byteSize = len(value)
	}
	col.colType = TypeBytes
	return col
}

func NewNullBytesColumn() *BytesColumn {
	col := &BytesColumn{}
	col.isNull = true
	col.colType = TypeBytes
	col.byteSize = 0
	return col
}

func (c *BytesColumn) GetAsString() string {
	if c.isNull {
		return ""
	}
	return string(c.value.([]byte))
}

func (c *BytesColumn) GetAsLong() (int64, error) {
	if c.isNull {
		return 0, fmt.Errorf("null value cannot be converted to long")
	}
	return 0, fmt.Errorf("bytes cannot be converted to long")
}

func (c *BytesColumn) GetAsDouble() (float64, error) {
	if c.isNull {
		return 0, fmt.Errorf("null value cannot be converted to double")
	}
	return 0, fmt.Errorf("bytes cannot be converted to double")
}

func (c *BytesColumn) GetAsDate() (time.Time, error) {
	if c.isNull {
		return time.Time{}, fmt.Errorf("null value cannot be converted to date")
	}
	return time.Time{}, fmt.Errorf("bytes cannot be converted to date")
}

func (c *BytesColumn) GetAsBool() (bool, error) {
	if c.isNull {
		return false, fmt.Errorf("null value cannot be converted to bool")
	}
	return false, fmt.Errorf("bytes cannot be converted to bool")
}

func (c *BytesColumn) GetAsBytes() ([]byte, error) {
	if c.isNull {
		return nil, fmt.Errorf("null value cannot be converted to bytes")
	}
	return c.value.([]byte), nil
}