package rabbitmq

import (
	"sync"
	"time"

	"github.com/streadway/amqp"
	"golang.org/x/sync/singleflight"
)

// ClientOption 客户端选项
type ClientOption func(*Client)

// Client 客户端
type Client struct {
	dsn           string
	vhost         string
	channelMax    int
	heartbeat     time.Duration
	retryInterval time.Duration // 重试时间间隔

	connection      *amqp.Connection    // RabbitMQ 连接
	serverCloseChan chan *amqp.Error    // 服务器端关闭事件通道
	clientCloseChan chan struct{}       // 客户端关闭事件通道
	syncWorkGroup   *singleflight.Group // 排他操作群组，保证同一时间只有一个协程处理任务
	locker          sync.RWMutex        // 读写锁，同步数据操作
	state           int8                // 客户端状态

	consumers map[*Consumer]struct{}
}

//
// Open
// @desc 开启
// @receiver c *Client
// @return error
//
func (c *Client) Open() error {
	if _, err := c.createConnection(); err != nil {
		return err
	}

	return nil
}

//
// listenCloseEvent
// @desc 监听关闭事件
// @receiver c *Client
//
func (c *Client) listenCloseEvent() {
	select {
	// 主动关闭
	case <-c.clientCloseChan:

	// 服务端关闭
	case <-c.serverCloseChan:
		c.locker.Lock()
		c.state = StateRetry
		c.locker.Unlock()

		_, _ = c.createConnection()
	}
}

//
// createConnection
// @desc 创建连接
// @receiver c *Client
// @return *amqp.Connection
// @return error
//
func (c *Client) createConnection() (*amqp.Connection, error) {
	connection, err, _ := c.syncWorkGroup.Do(createConnectionTaskKey, func() (interface{}, error) {
		for {
			select {
			// 主动关闭
			case <-c.clientCloseChan:
				return nil, ErrClientClosed

			default:
				var err error
				var connection *amqp.Connection

				if connection, err = c.checkAliveConnection(); err == nil {
					return connection, nil
				}

				if err != ErrConnectionCreateRequired {
					return nil, err
				}

				if connection, err = c.dial(); err != nil {
					time.Sleep(c.retryInterval)

					continue
				}

				if err = c.activateClient(connection); err != nil {
					return nil, err
				}

				return connection, nil
			}
		}
	})

	if err != nil {
		return nil, err
	}

	return connection.(*amqp.Connection), nil
}

//
// checkAliveConnection
// @desc 检查存活连接
// @receiver c *Client
// @return *amqp.Connection
// @return error
//
func (c *Client) checkAliveConnection() (*amqp.Connection, error) {
	c.locker.RLock()

	defer c.locker.RUnlock()

	if c.state == StateClosed {
		return nil, ErrClientClosed
	}

	if c.connection != nil && !c.connection.IsClosed() {
		return c.connection, nil
	}

	return nil, ErrConnectionCreateRequired
}

//
// activateClient
// @desc 激活客户端
// @receiver c *Client
// @param connection *amqp.Connection
// @return error
//
func (c *Client) activateClient(connection *amqp.Connection) error {
	c.locker.Lock()

	defer c.locker.Unlock()

	// 客户端已关闭
	if c.state == StateClosed {
		_ = connection.Close()

		return ErrClientClosed
	}

	c.connection = connection
	c.state = StateActive
	c.serverCloseChan = make(chan *amqp.Error)

	c.connection.NotifyClose(c.serverCloseChan)

	go c.listenCloseEvent()

	return nil
}

//
// dial
// @desc 连接服务器
// @receiver c *Client
// @return *amqp.Connection
// @return error
//
func (c *Client) dial() (*amqp.Connection, error) {
	config := amqp.Config{
		Vhost:      c.vhost,
		ChannelMax: c.channelMax,
		Heartbeat:  c.heartbeat,
		Locale:     defaultLocale,
	}

	return amqp.DialConfig(c.dsn, config)
}

//
// Connection
// @desc 获取连接
// @receiver c *Client
// @return *amqp.Connection
// @return error
//
func (c *Client) Connection() (*amqp.Connection, error) {
	connection, err := c.checkAliveConnection()

	if err == nil {
		return connection, nil
	}

	if err == ErrConnectionCreateRequired {
		return c.createConnection()
	}

	return nil, err
}

//
// Channel
// @desc 获取 CHANNEL
// @receiver c *Client
// @return *amqp.Channel
// @return error
//
func (c *Client) Channel() (*amqp.Channel, error) {
	connection, err := c.Connection()

	if err != nil {
		return nil, err
	}

	return connection.Channel()
}

//
// Declare
// @desc 声明
// @receiver c *Client
// @param declaration Declaration
// @return error
//
func (c *Client) Declare(declaration Declaration) error {
	channel, err := c.Channel()

	if err != nil {
		return err
	}

	defer func() {
		_ = channel.Close()
	}()

	return declaration(channel)
}

//
// State
// @desc 获取客户端状态
// @receiver c *Client
// @return int8
//
func (c *Client) State() int8 {
	c.locker.RLock()

	defer c.locker.RUnlock()

	return c.state
}

//
// Store
// @desc 恢复
// @receiver c *Client
// @return error
//
func (c *Client) Store() error {
	c.locker.RLock()

	if c.state != StateClosed {
		c.locker.RUnlock()

		return ErrClientNotClosed
	}

	c.state = StateInit
	c.clientCloseChan = make(chan struct{})

	c.locker.RUnlock()

	if _, err := c.createConnection(); err != nil {
		return err
	}

	return nil
}

//
// Close
// @desc 关闭客户端
// @receiver c *Client
//
func (c *Client) Close() {
	c.locker.Lock()

	defer c.locker.Unlock()

	if c.state == StateClosed {
		return
	}

	close(c.clientCloseChan)

	if c.connection != nil {
		_ = c.connection.Close()
	}

	c.state = StateClosed
}

//
// NewClient
// @desc 创建客户端实例
// @param dsn string
// @param options []ClientOption
// @return *Client
//
func NewClient(dsn string, options ...ClientOption) *Client {
	c := &Client{
		dsn:             dsn,
		channelMax:      defaultChannelMax,
		heartbeat:       defaultHeartbeat,
		retryInterval:   defalutRetryInterval,
		clientCloseChan: make(chan struct{}),
		syncWorkGroup:   new(singleflight.Group),
		state:           StateInit,
	}

	for _, option := range options {
		option(c)
	}

	return c

}

//
// WithClientVhost
// @desc 配置虚拟主机
// @param vhost string
// @return ClientOption
//
func WithClientVhost(vhost string) ClientOption {
	return func(c *Client) {
		c.vhost = vhost
	}
}

//
// WithClientChannelMax
// @desc 配置最大 CHANNEL 数量
// @param channelMax int
// @return ClientOption
//
func WithClientChannelMax(channelMax int) ClientOption {
	return func(c *Client) {
		c.channelMax = channelMax
	}
}

//
// WithClientHeartbeat
// @desc 配置心跳间隔
// @param heartbeat time.Duration
// @return ClientOption
//
func WithClientHeartbeat(heartbeat time.Duration) ClientOption {
	return func(c *Client) {
		c.heartbeat = heartbeat
	}
}

//
// WithClientRetryInterval
// @desc 配置重连间隔
// @param retryInterval time.Duration
// @return ClientOption
//
func WithClientRetryInterval(retryInterval time.Duration) ClientOption {
	return func(c *Client) {
		c.retryInterval = retryInterval
	}
}
