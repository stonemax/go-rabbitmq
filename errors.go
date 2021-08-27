package rabbitmq

import (
	"errors"
)

var (
	// ErrClientClosed 客户端已关闭
	ErrClientClosed = errors.New("client is closed")

	// ErrClientNotClosed 客户端不处于关闭状态
	ErrClientNotClosed = errors.New("client isn't closed")

	// ErrConnectionCreateRequired 需要创建连接
	ErrConnectionCreateRequired = errors.New("connection is required to create")
)

var (
	// ErrConsumerNotInit 消费者不处于初始状态
	ErrConsumerNotInit = errors.New("consumer isn't in init stratus")

	// ErrConsumerClosed 消费者已关闭
	ErrConsumerClosed = errors.New("consumer is closed")

	// ErrChannelCreateRequired 需要创建 CHANNEL
	ErrChannelCreateRequired = errors.New("channel is required to create")
)
