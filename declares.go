package rabbitmq

import (
	"github.com/streadway/amqp"
)

// Declaration 声明器
type Declaration func(Declarer) error

// Declarer 声明接口
type Declarer interface {
	// ExchangeDeclare 声明交换机
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error

	// QueueDeclare 声明队列
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)

	// QueueBind 绑定队列
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
}

//
// DeclareExchange
// @desc 声明交换机
// @param e *Exchange
// @return Declaration
//
func DeclareExchange(e *Exchange) Declaration {
	return func(d Declarer) error {
		return d.ExchangeDeclare(
			e.Name,
			e.Type,
			e.Durable,
			e.AutoDelete,
			false,
			e.NoWait,
			e.Args,
		)
	}
}

//
// DeclareQueue
// @desc 声明队列
// @param q *Queue
// @return Declaration
//
func DeclareQueue(q *Queue) Declaration {
	return func(d Declarer) error {
		sq, err := d.QueueDeclare(
			q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			q.NoWait,
			q.Args,
		)

		if err != nil {
			return err
		}

		q.Name = sq.Name

		return nil
	}
}

//
// DeclareBinding
// @desc 声明绑定
// @param b *Binding
// @return Declaration
//
func DeclareBinding(b *Binding) Declaration {
	return func(d Declarer) error {
		return d.QueueBind(
			b.Queue.Name,
			b.Key,
			b.Exchange.Name,
			b.NoWait,
			b.Args,
		)
	}
}
