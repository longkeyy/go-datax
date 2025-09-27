package statistics

import (
	"fmt"
	"github.com/longkeyy/go-datax/common/config"
	"sync"
)

// ErrorLimiter 错误控制器
type ErrorLimiter struct {
	mutex sync.RWMutex

	// 配置参数
	maxErrorRecord     int     // 最大错误记录数
	maxErrorPercentage float64 // 最大错误百分比

	// 统计数据
	totalRecords  int64 // 总记录数
	errorRecords  int64 // 错误记录数
	filterRecords int64 // 过滤记录数
}

// NewErrorLimiter 创建错误控制器
func NewErrorLimiter(configuration *config.Configuration) *ErrorLimiter {
	errorLimit := configuration.GetConfiguration("job.setting.errorLimit")

	maxErrorRecord := errorLimit.GetIntWithDefault("record", 0)
	maxErrorPercentage := float64(errorLimit.GetIntWithDefault("percentage", 100)) / 100.0

	return &ErrorLimiter{
		maxErrorRecord:     maxErrorRecord,
		maxErrorPercentage: maxErrorPercentage,
	}
}

// AddTotalRecord 增加总记录数
func (el *ErrorLimiter) AddTotalRecord(count int64) {
	el.mutex.Lock()
	defer el.mutex.Unlock()
	el.totalRecords += count
}

// AddErrorRecord 增加错误记录数
func (el *ErrorLimiter) AddErrorRecord(count int64) error {
	el.mutex.Lock()
	defer el.mutex.Unlock()

	el.errorRecords += count

	// 检查错误限制
	return el.checkErrorLimit()
}

// AddFilterRecord 增加过滤记录数
func (el *ErrorLimiter) AddFilterRecord(count int64) {
	el.mutex.Lock()
	defer el.mutex.Unlock()
	el.filterRecords += count
}

// checkErrorLimit 检查是否超过错误限制
func (el *ErrorLimiter) checkErrorLimit() error {
	// 检查错误记录数限制
	if el.maxErrorRecord > 0 && el.errorRecords > int64(el.maxErrorRecord) {
		return fmt.Errorf("error record count (%d) exceeds limit (%d)",
			el.errorRecords, el.maxErrorRecord)
	}

	// 检查错误百分比限制
	if el.totalRecords > 0 && el.maxErrorPercentage < 1.0 {
		errorPercentage := float64(el.errorRecords) / float64(el.totalRecords)
		if errorPercentage > el.maxErrorPercentage {
			return fmt.Errorf("error percentage (%.2f%%) exceeds limit (%.2f%%)",
				errorPercentage*100, el.maxErrorPercentage*100)
		}
	}

	return nil
}

// GetStatistics 获取统计信息
func (el *ErrorLimiter) GetStatistics() Statistics {
	el.mutex.RLock()
	defer el.mutex.RUnlock()

	return Statistics{
		TotalRecords:  el.totalRecords,
		ErrorRecords:  el.errorRecords,
		FilterRecords: el.filterRecords,
		SuccessRecords: el.totalRecords - el.errorRecords,
	}
}

// IsErrorLimitExceeded 检查是否超过错误限制（只检查不更新）
func (el *ErrorLimiter) IsErrorLimitExceeded() bool {
	el.mutex.RLock()
	defer el.mutex.RUnlock()

	// 检查错误记录数限制
	if el.maxErrorRecord > 0 && el.errorRecords >= int64(el.maxErrorRecord) {
		return true
	}

	// 检查错误百分比限制
	if el.totalRecords > 0 && el.maxErrorPercentage < 1.0 {
		errorPercentage := float64(el.errorRecords) / float64(el.totalRecords)
		if errorPercentage >= el.maxErrorPercentage {
			return true
		}
	}

	return false
}

// Statistics 统计信息
type Statistics struct {
	TotalRecords   int64 // 总记录数
	SuccessRecords int64 // 成功记录数
	ErrorRecords   int64 // 错误记录数
	FilterRecords  int64 // 过滤记录数
}

// GetErrorPercentage 获取错误百分比
func (s *Statistics) GetErrorPercentage() float64 {
	if s.TotalRecords == 0 {
		return 0.0
	}
	return float64(s.ErrorRecords) / float64(s.TotalRecords) * 100
}

// GetFilterPercentage 获取过滤百分比
func (s *Statistics) GetFilterPercentage() float64 {
	if s.TotalRecords == 0 {
		return 0.0
	}
	return float64(s.FilterRecords) / float64(s.TotalRecords) * 100
}

// String 返回统计信息的字符串表示
func (s *Statistics) String() string {
	return fmt.Sprintf("Total: %d, Success: %d, Error: %d (%.2f%%), Filter: %d (%.2f%%)",
		s.TotalRecords, s.SuccessRecords, s.ErrorRecords, s.GetErrorPercentage(),
		s.FilterRecords, s.GetFilterPercentage())
}