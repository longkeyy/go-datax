package statistics

import (
	"sync"
	"time"
)

// State 状态枚举
type State int

const (
	StateWaiting State = iota
	StateRunning
	StateSucceed
	StateSucceeded // 添加StateSucceeded别名
	StateFailed
)

// String 实现State的字符串表示
func (s State) String() string {
	switch s {
	case StateWaiting:
		return "WAITING"
	case StateRunning:
		return "RUNNING"
	case StateSucceed, StateSucceeded:
		return "SUCCEEDED"
	case StateFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// Communication 统计通信结构体
type Communication struct {
	// 基础信息
	timestamp  int64
	state      State
	throwable  error

	// 计数器
	longCounters map[string]int64

	// 并发安全
	mu sync.RWMutex
}

// NewCommunication 创建新的Communication实例
func NewCommunication() *Communication {
	return &Communication{
		timestamp:    time.Now().UnixMilli(),
		state:        StateWaiting,
		longCounters: make(map[string]int64),
	}
}

// GetTimestamp 获取时间戳
func (c *Communication) GetTimestamp() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.timestamp
}

// SetTimestamp 设置时间戳
func (c *Communication) SetTimestamp(timestamp int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timestamp = timestamp
}

// GetState 获取状态
func (c *Communication) GetState() State {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// SetState 设置状态
func (c *Communication) SetState(state State) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = state
}

// GetThrowable 获取异常
func (c *Communication) GetThrowable() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.throwable
}

// SetThrowable 设置异常
func (c *Communication) SetThrowable(throwable error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.throwable = throwable
}

// GetLongCounter 获取长整型计数器
func (c *Communication) GetLongCounter(key string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.longCounters[key]
}

// SetLongCounter 设置长整型计数器
func (c *Communication) SetLongCounter(key string, value int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.longCounters[key] = value
}

// IncreaseCounter 增加计数器
func (c *Communication) IncreaseCounter(key string, delta int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.longCounters[key] += delta
}

// Reset 重置Communication
func (c *Communication) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timestamp = time.Now().UnixMilli()
	c.state = StateWaiting
	c.throwable = nil
	c.longCounters = make(map[string]int64)
}

// Clone 克隆Communication
func (c *Communication) Clone() *Communication {
	c.mu.RLock()
	defer c.mu.RUnlock()

	newComm := &Communication{
		timestamp:    c.timestamp,
		state:        c.state,
		throwable:    c.throwable,
		longCounters: make(map[string]int64),
	}

	for k, v := range c.longCounters {
		newComm.longCounters[k] = v
	}

	return newComm
}

// GetCounters 获取所有计数器（只读副本）
func (c *Communication) GetCounters() map[string]int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]int64)
	for k, v := range c.longCounters {
		result[k] = v
	}
	return result
}

// MergeFrom 从另一个Communication合并数据
func (c *Communication) MergeFrom(other *Communication) {
	if other == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	// 合并计数器
	for k, v := range other.longCounters {
		c.longCounters[k] += v
	}

	// 更新时间戳为最新的
	if other.timestamp > c.timestamp {
		c.timestamp = other.timestamp
	}

	// 如果有异常，保留异常
	if other.throwable != nil {
		c.throwable = other.throwable
	}

	// 状态更新逻辑：失败状态优先，然后是成功，最后是运行中
	if other.state == StateFailed {
		c.state = StateFailed
	} else if c.state != StateFailed && other.state == StateSucceed {
		c.state = StateSucceed
	} else if c.state == StateWaiting && other.state == StateRunning {
		c.state = StateRunning
	}
}

// GetThrowableMessage 获取异常消息
func (c *Communication) GetThrowableMessage() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.throwable != nil {
		return c.throwable.Error()
	}
	return ""
}