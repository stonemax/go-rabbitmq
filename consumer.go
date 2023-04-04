package rabbitmq

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/streadway/amqp"
)

// ConsumerOption 消费者选项
type ConsumerOption func(*Consumer)

// Consumer 消费者
type Consumer struct {
	queues    []string
	tags      []string // 消费者标签
	qos       int
	autoACK   bool
	exclusive bool
	noLocal   bool
	noWait    bool
	args      amqp.Table

	client          *Client            // 客户端
	channel         *amqp.Channel      // CHANNEL
	dataChan        chan amqp.Delivery // 数据通道
	dataChanWG      *sync.WaitGroup    // 数据通道 WaitGroup
	serverCloseChan chan *amqp.Error   // 服务器端关闭事件通道
	clientCloseChan chan struct{}      // 客户端关闭事件通道
	locker          sync.RWMutex       // 锁
	state           int8               // 消费者状态
}

//
// Serve
// @desc 运行
// @receiver c *Consumer
// @return error
//
func (c *Consumer) Serve() error {
	// 消费者不处于初始状态
	if c.State() != StateInit {
		return ErrConsumerNotInit
	}

	// 执行消费
	if _, err := c.createChannel(); err != nil {
		return err
	}

	return nil
}

//
// createChannel
// @desc 创建 CHANNEL
// @receiver c *Consumer
// @return *amqp.Channel
// @return error
//
func (c *Consumer) createChannel() (*amqp.Channel, error) {
	select {
	case <-c.clientCloseChan:
		return nil, ErrConsumerClosed

	default:
		var err error
		var channel *amqp.Channel
		var dataChans []<-chan amqp.Delivery

		// 消费队列
		if channel, dataChans, err = c.consumeQueue(); err != nil {
			return nil, err
		}

		// 激活消费者
		if err = c.activateConsumer(channel, dataChans); err != nil {
			return nil, err
		}

		return channel, nil
	}
}

//
// consumeQueue
// @desc 消费队列
// @receiver c *Consumer
// @return *amqp.Channel
// @return []<-chan amqp.Delivery
// @return error
//
func (c *Consumer) consumeQueue() (*amqp.Channel, []<-chan amqp.Delivery, error) {
	var err error
	var channel *amqp.Channel
	var dataChans []<-chan amqp.Delivery

	// 阻塞获取 CHANNEL
	if channel, err = c.client.Channel(); err != nil {
		return nil, nil, err
	}

	// 设置 QoS
	if c.qos > 0 {
		if err = channel.Qos(c.qos, 0, false); err != nil {
			return nil, nil, err
		}
	}

	// 消费队列
	for index, queue := range c.queues {
		ch, err := channel.Consume(
			queue,
			c.tags[index],
			c.autoACK,
			c.exclusive,
			c.noLocal,
			c.noWait,
			c.args,
		)

		if err != nil {
			_ = channel.Close()

			return nil, nil, err
		}

		dataChans = append(dataChans, ch)
	}

	return channel, dataChans, nil
}

//
// activateConsumer
// @desc 激活消费者
// @receiver c *Consumer
// @param channel *amqp.Channel
// @param dataChans []<-chan amqp.Delivery
// @return error
//
func (c *Consumer) activateConsumer(channel *amqp.Channel, dataChans []<-chan amqp.Delivery) error {
	c.locker.Lock()

	defer c.locker.Unlock()

	if c.state == StateClosed {
		_ = channel.Close()

		return ErrConsumerClosed
	}

	c.channel = channel
	c.state = StateActive
	c.serverCloseChan = make(chan *amqp.Error)

	c.deliveryData(dataChans)
	c.channel.NotifyClose(c.serverCloseChan)

	// 监听关闭事件
	go c.listenCloseEvent()

	return nil
}

//
// deliveryData
// @desc 传输数据
// @receiver c *Consumer
// @param dataChans []<-chan amqp.Delivery
//
func (c *Consumer) deliveryData(dataChans []<-chan amqp.Delivery) {
	for _, ch := range dataChans {
		go func(ch <-chan amqp.Delivery) {
			c.dataChanWG.Add(1)

			defer c.dataChanWG.Done()

			for {
				select {
				// 客户端关闭
				case <-c.clientCloseChan:
					return

				case data, ok := <-ch:
					// CHANNEL 关闭
					if !ok {
						return
					}

					c.dataChan <- data
				}
			}
		}(ch)
	}
}

//
// listenCloseEvent
// @desc 监听关闭事件
// @receiver c *Consumer
//
func (c *Consumer) listenCloseEvent() {
	select {
	// 客户端关闭
	case <-c.clientCloseChan:

	// 服务端关闭
	case <-c.serverCloseChan:
		c.locker.Lock()
		c.state = StateRetry
		c.locker.Unlock()

		_, err := c.createChannel()

		if err == nil || err == ErrConsumerClosed {
			return
		}

		c.Close()
	}
}

//
// DataChan
// @desc 获取数据传输通道
// @receiver c *Consumer
// @return <-chan
//
func (c *Consumer) DataChan() <-chan amqp.Delivery {
	return c.dataChan
}

//
// closeDataChan
// @desc 关闭数据通道
// @receiver c *Consumer
//
func (c *Consumer) closeDataChan() {
	c.dataChanWG.Wait()

	close(c.dataChan)
}

//
// State
// @desc 获取状态
// @receiver c *Consumer
// @return int8
//
func (c *Consumer) State() int8 {
	c.locker.RLock()

	defer c.locker.RUnlock()

	return c.state
}

//
// Close
// @desc 关闭
// @receiver c *Consumer
//
func (c *Consumer) Close() {
	c.locker.Lock()

	defer c.locker.Unlock()

	// 若为已关闭状态直接返回
	if c.state == StateClosed {
		return
	}

	close(c.clientCloseChan)

	if c.channel != nil {
		_ = c.channel.Close()
	}

	c.closeDataChan()
	c.state = StateClosed
}

//
// NewConsumer
// @desc 创建消费者
// @param client *Client
// @param queues []string
// @param options []ConsumerOption
// @return *Consumer
//
func NewConsumer(client *Client, queues []string, options ...ConsumerOption) *Consumer {
	consumer := &Consumer{
		queues:          queues,
		client:          client,
		dataChan:        make(chan amqp.Delivery),
		dataChanWG:      new(sync.WaitGroup),
		clientCloseChan: make(chan struct{}),
		state:           StateInit,
	}

	WithConsumerAutoTag()(consumer)

	for _, option := range options {
		option(consumer)
	}

	return consumer
}

//
// WithConsumerTag
// @desc 配置消费者标签
// @param tags string
// @return ConsumerOption
//
func WithConsumerTag(tags ...string) ConsumerOption {
	return func(consumer *Consumer) {
		consumer.tags = tags
	}
}

//
// WithConsumerAutoTag
// @desc 自动配置消费者标签
// @return ConsumerOption
//
func WithConsumerAutoTag() ConsumerOption {
	return func(c *Consumer) {
		hostname, _ := os.Hostname()
		processId := os.Getpid()

		prefix := fmt.Sprintf("[%s]-[pid-%d]@%s", strings.Join(c.queues, "|"), processId, hostname)

		var tags []string

		for index := range c.queues {
			tags = append(tags, fmt.Sprintf("%s-%d", prefix, index))
		}

		WithConsumerTag(tags...)(c)
	}
}

//
// WithConsumerQOS
// @desc 配置 QOS(PRE-FETCH)
// @param qos int
// @return ConsumerOption
//
func WithConsumerQOS(qos int) ConsumerOption {
	return func(c *Consumer) {
		c.qos = qos
	}
}

//
// WithConsumerAutoACK
// @desc 配置自动 ACK
// @return ConsumerOption
//
func WithConsumerAutoACK() ConsumerOption {
	return func(c *Consumer) {
		c.autoACK = true
	}
}

//
// WithConsumerExclusive
// @desc 配置独占使用
// @return ConsumerOption
//
func WithConsumerExclusive() ConsumerOption {
	return func(c *Consumer) {
		c.exclusive = true
	}
}

//
// WithConsumerNoLocal
// @desc 配置 NO-LOCAL
// @return ConsumerOption
//
func WithConsumerNoLocal() ConsumerOption {
	return func(c *Consumer) {
		c.noLocal = true
	}
}

//
// WithConsumerNoWait
// @desc 配置 NO-WAIT
// @return ConsumerOption
//
func WithConsumerNoWait() ConsumerOption {
	return func(c *Consumer) {
		c.noWait = true
	}
}

//
// WithConsumerArgs
// @desc 配置额外选项
// @param args amqp.Table
// @return ConsumerOption
//
func WithConsumerArgs(args amqp.Table) ConsumerOption {
	return func(c *Consumer) {
		c.args = args
	}
}
