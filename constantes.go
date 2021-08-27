package rabbitmq

import (
	"time"
)

const (
	// defaultChannelMax 默认 CHANNEL 最大数量
	defaultChannelMax = (2 << 10) - 1

	// defaultHeartbeat 默认心跳间隔
	defaultHeartbeat = 10 * time.Second

	// defaultLocale 默认区域
	defaultLocale = "en_US"

	// defalutRetryInterval 客户端默认重连间隔时间
	defalutRetryInterval = 3 * time.Second

	// createConnectionTaskKey 创建连接任务键名
	createConnectionTaskKey = "CreateConnection"
)

const (
	// StateInit 初始化状态
	StateInit int8 = iota + 1

	// StateActive 活跃状态
	StateActive

	// StatePaused 暂停状态
	StatePaused

	// StateRetry 重试状态
	StateRetry

	// StateClosed 关闭状态
	StateClosed
)
