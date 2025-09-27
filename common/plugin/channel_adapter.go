package plugin

import "github.com/longkeyy/go-datax/common/element"

// ChannelAdapter 通道适配器，将TransformerChannel适配为Channel接口
type ChannelAdapter struct {
	transformerChannel *TransformerChannel
}

func NewChannelAdapter(transformerChannel *TransformerChannel) *ChannelAdapter {
	return &ChannelAdapter{
		transformerChannel: transformerChannel,
	}
}

func (ca *ChannelAdapter) Close() error {
	return ca.transformerChannel.Close()
}

func (ca *ChannelAdapter) Push(record element.Record) error {
	return ca.transformerChannel.Push(record)
}

func (ca *ChannelAdapter) Pull() (element.Record, error) {
	return ca.transformerChannel.Pull()
}

func (ca *ChannelAdapter) Size() int {
	return ca.transformerChannel.Size()
}

func (ca *ChannelAdapter) IsClosed() bool {
	return ca.transformerChannel.IsClosed()
}