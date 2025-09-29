package element

import (
	"fmt"
)

// Record represents a single row of data with columnar structure.
// Provides operations for column manipulation and data access.
type Record interface {
	AddColumn(column Column)
	SetColumn(index int, column Column)
	GetColumn(index int) Column
	GetColumnNumber() int
	GetByteSize() int
	String() string
}

// RecordFactory provides abstraction for creating Record instances.
type RecordFactory interface {
	CreateRecord() Record
}

// DefaultRecordFactory provides the standard implementation for record creation.
type DefaultRecordFactory struct{}

func NewRecordFactory() RecordFactory {
	return &DefaultRecordFactory{}
}

func (f *DefaultRecordFactory) CreateRecord() Record {
	return NewRecord()
}

// DefaultRecord provides the standard implementation of the Record interface
// with dynamic column storage and efficient memory management.
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