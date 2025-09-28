package statistics

import (
	"encoding/json"
	"sync"
	"time"
)

// State 表示任务运行状态
type State int

const (
	StateRunning State = iota
	StateSucceeded
	StateFailed
	StateKilled
	StateKilling
)

// IsRunning 检查状态是否为运行中
func (s State) IsRunning() bool {
	return s == StateRunning
}

// IsFinished 检查状态是否已完成
func (s State) IsFinished() bool {
	return s == StateSucceeded || s == StateFailed || s == StateKilled
}

func (s State) String() string {
	switch s {
	case StateRunning:
		return "RUNNING"
	case StateSucceeded:
		return "SUCCEEDED"
	case StateFailed:
		return "FAILED"
	case StateKilled:
		return "KILLED"
	case StateKilling:
		return "KILLING"
	default:
		return "UNKNOWN"
	}
}

// Communication DataX所有的状态及统计信息交互类，job、taskGroup、task等的消息汇报都走该类
type Communication struct {
	mu sync.RWMutex

	// 所有的数值key-value对
	counter map[string]int64

	// 运行状态
	state State

	// 异常记录
	throwable error

	// 记录的timestamp
	timestamp int64

	// task给job的信息
	message map[string][]string
}

// NewCommunication 创建新的Communication实例
func NewCommunication() *Communication {
	return &Communication{
		counter:   make(map[string]int64),
		state:     StateRunning,
		timestamp: time.Now().UnixMilli(),
		message:   make(map[string][]string),
	}
}

// Reset 重置Communication
func (c *Communication) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.init()
}

func (c *Communication) init() {
	c.counter = make(map[string]int64)
	c.state = StateRunning
	c.throwable = nil
	c.message = make(map[string][]string)
	c.timestamp = time.Now().UnixMilli()
}

// GetCounter 获取所有计数器
func (c *Communication) GetCounter() map[string]int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]int64)
	for k, v := range c.counter {
		result[k] = v
	}
	return result
}

// GetState 获取状态
func (c *Communication) GetState() State {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// SetState 设置状态
func (c *Communication) SetState(state State) {
	c.SetStateForce(state, false)
}

// SetStateForce 强制设置状态
func (c *Communication) SetStateForce(state State, force bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !force && c.state == StateFailed {
		return
	}
	c.state = state
}

// GetThrowable 获取异常
func (c *Communication) GetThrowable() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.throwable
}

// GetThrowableMessage 获取异常消息
func (c *Communication) GetThrowableMessage() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.throwable == nil {
		return ""
	}
	return c.throwable.Error()
}

// SetThrowable 设置异常
func (c *Communication) SetThrowable(throwable error) {
	c.SetThrowableForce(throwable, false)
}

// SetThrowableForce 强制设置异常
func (c *Communication) SetThrowableForce(throwable error, force bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if force {
		c.throwable = throwable
	} else {
		if c.throwable == nil {
			c.throwable = throwable
		}
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

// GetMessage 获取所有消息
func (c *Communication) GetMessage() map[string][]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string][]string)
	for k, v := range c.message {
		result[k] = make([]string, len(v))
		copy(result[k], v)
	}
	return result
}

// GetMessageByKey 根据key获取消息
func (c *Communication) GetMessageByKey(key string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if msgs, exists := c.message[key]; exists {
		result := make([]string, len(msgs))
		copy(result, msgs)
		return result
	}
	return nil
}

// AddMessage 添加消息
func (c *Communication) AddMessage(key, value string) {
	if key == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.message[key] == nil {
		c.message[key] = make([]string, 0)
	}
	c.message[key] = append(c.message[key], value)
}

// GetLongCounter 获取长整型计数器值
func (c *Communication) GetLongCounter(key string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if value, exists := c.counter[key]; exists {
		return value
	}
	return 0
}

// SetLongCounter 设置长整型计数器值
func (c *Communication) SetLongCounter(key string, value int64) {
	if key == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.counter[key] = value
}

// IncreaseCounter 增加计数器值
func (c *Communication) IncreaseCounter(key string, deltaValue int64) {
	if key == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if value, exists := c.counter[key]; exists {
		c.counter[key] = value + deltaValue
	} else {
		c.counter[key] = deltaValue
	}
}

// Clone 克隆Communication
func (c *Communication) Clone() *Communication {
	c.mu.RLock()
	defer c.mu.RUnlock()

	newComm := NewCommunication()

	// 克隆counter
	for k, v := range c.counter {
		newComm.counter[k] = v
	}

	// 克隆状态和异常
	newComm.state = c.state
	newComm.throwable = c.throwable
	newComm.timestamp = c.timestamp

	// 克隆消息
	for k, v := range c.message {
		newComm.message[k] = make([]string, len(v))
		copy(newComm.message[k], v)
	}

	return newComm
}

// MergeFrom 合并另一个Communication的信息
func (c *Communication) MergeFrom(other *Communication) *Communication {
	if other == nil {
		return c
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	// 合并counter，将other的值累加到this中
	for key, value := range other.counter {
		if existingValue, exists := c.counter[key]; exists {
			c.counter[key] = existingValue + value
		} else {
			c.counter[key] = value
		}
	}

	// 合并状态 - 优先级： (Failed | Killed) > Running > Success
	c.mergeStateFrom(other.state)

	// 合并throwable，当this的throwable为空时，才将other的throwable合并进来
	if c.throwable == nil {
		c.throwable = other.throwable
	}

	// 合并消息
	for key, values := range other.message {
		if c.message[key] == nil {
			c.message[key] = make([]string, 0)
		}
		c.message[key] = append(c.message[key], values...)
	}

	return c
}

// mergeStateFrom 合并状态
func (c *Communication) mergeStateFrom(otherState State) State {
	if c.state == StateFailed || otherState == StateFailed ||
		c.state == StateKilled || otherState == StateKilled {
		c.state = StateFailed
	} else if c.state.IsRunning() || otherState.IsRunning() {
		c.state = StateRunning
	}
	return c.state
}

// IsFinished 检查是否已完成
func (c *Communication) IsFinished() bool {
	return c.GetState().IsFinished()
}

// ToJSON 转换为JSON字符串
func (c *Communication) ToJSON() (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	data := map[string]interface{}{
		"counter":   c.counter,
		"state":     c.state.String(),
		"timestamp": c.timestamp,
		"message":   c.message,
	}

	if c.throwable != nil {
		data["throwable"] = c.throwable.Error()
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}