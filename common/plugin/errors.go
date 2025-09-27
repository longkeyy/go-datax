package plugin

import "errors"

var (
	ErrChannelClosed = errors.New("channel is closed")
	ErrRecordNil     = errors.New("record is nil")
	ErrConfigNil     = errors.New("configuration is nil")
	ErrPluginInit    = errors.New("plugin initialization failed")
)