package rabbitmq

import (
	"github.com/streadway/amqp"
)

// Queue 队列
type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

// Exchange 交换机
type Exchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	NoWait     bool
	Args       amqp.Table
}

// Binding 绑定
type Binding struct {
	Queue    *Queue
	Exchange *Exchange
	Key      string
	NoWait   bool
	Args     amqp.Table
}
