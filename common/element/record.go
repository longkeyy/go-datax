package element

import (
	"fmt"
)

// Record 记录接口，表示一行数据
type Record interface {
	AddColumn(column Column)
	SetColumn(index int, column Column)
	GetColumn(index int) Column
	GetColumnNumber() int
	GetByteSize() int
	String() string
}

// DefaultRecord 默认记录实现
type DefaultRecord struct {
	columns []Column
}

func NewRecord() *DefaultRecord {
	return &DefaultRecord{
		columns: make([]Column, 0),
	}
}

func (r *DefaultRecord) AddColumn(column Column) {
	r.columns = append(r.columns, column)
}

func (r *DefaultRecord) SetColumn(index int, column Column) {
	if index < 0 {
		panic(fmt.Sprintf("设置column的index不能小于0，index: %d", index))
	}

	// 扩展slice如果需要
	for len(r.columns) <= index {
		r.columns = append(r.columns, NewStringColumn(""))
	}

	r.columns[index] = column
}

func (r *DefaultRecord) GetColumn(index int) Column {
	if index < 0 || index >= len(r.columns) {
		return nil
	}
	return r.columns[index]
}

func (r *DefaultRecord) GetColumnNumber() int {
	return len(r.columns)
}

func (r *DefaultRecord) GetByteSize() int {
	size := 0
	for _, col := range r.columns {
		size += col.GetByteSize()
	}
	return size
}

func (r *DefaultRecord) String() string {
	result := "["
	for i, col := range r.columns {
		if i > 0 {
			result += ", "
		}
		if col.IsNull() {
			result += "null"
		} else {
			result += col.GetAsString()
		}
	}
	result += "]"
	return result
}