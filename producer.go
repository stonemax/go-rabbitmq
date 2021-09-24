package rabbitmq

import (
	"github.com/streadway/amqp"
)

// ProducerOption 生产者选项
type ProducerOption func(*Producer)

// Producer 生产者
type Producer struct {
	exchange   string
	routingKey string
	mandatory  bool
	immediate  bool
	template   amqp.Publishing

	client  *Client
	channel *amqp.Channel
}

//
// Publish
// @desc 发布
// @receiver p *Producer
// @param body []byte
// @param routingKeys []string
// @return error
//
func (p *Producer) Publish(body []byte, routingKeys ...string) error {
	data := p.template
	data.Body = body

	return p.PublishFullData(data, routingKeys...)
}

//
// PublishFullData
// @desc 发布独立数据
// @receiver p *Producer
// @param data amqp.Publishing
// @param routingKeys []string
// @return error
//
func (p *Producer) PublishFullData(data amqp.Publishing, routingKeys ...string) error {
	channel, err := p.client.Channel()

	defer func() {
		_ = channel.Close()
	}()

	if err != nil {
		return err
	}

	routingKey := p.routingKey

	if len(routingKeys) > 0 && len(routingKeys[0]) > 0 {
		routingKey = routingKeys[0]
	}

	return channel.Publish(
		p.exchange,
		routingKey,
		p.mandatory,
		p.immediate,
		data,
	)
}

//
// PublishMulti
// @desc 批量发布
// @receiver p *Producer
// @param bodies [][]byte
// @param routingKeys []string
// @return error
//
func (p *Producer) PublishMulti(bodies [][]byte, routingKeys ...string) error {
	var data []amqp.Publishing

	for _, body := range bodies {
		item := p.template
		item.Body = body

		data = append(data, item)
	}

	return p.PublishFullDataMulti(data, routingKeys...)
}

//
// PublishFullDataMulti
// @desc 批量发布独立数据
// @receiver p *Producer
// @param data []amqp.Publishing
// @param routingKeys []string
// @return error
//
func (p *Producer) PublishFullDataMulti(data []amqp.Publishing, routingKeys ...string) error {
	var err error
	var channel *amqp.Channel

	if channel, err = p.client.Channel(); err != nil {
		return err
	}

	defer func() {
		_ = channel.Close()
	}()

	routingKey := p.routingKey

	if len(routingKeys) > 0 && len(routingKeys[0]) > 0 {
		routingKey = routingKeys[0]
	}

	for _, item := range data {
		err = channel.Publish(
			p.exchange,
			routingKey,
			p.mandatory,
			p.immediate,
			item,
		)

		if err != nil {
			return err
		}
	}

	return nil
}

//
// NewProducer
// @desc 创建生产者
// @param client *Client
// @param exchange string
// @param routingKey string
// @param options []ProducerOption
// @return *Producer
//
func NewProducer(client *Client, exchange, routingKey string, options ...ProducerOption) *Producer {
	p := &Producer{
		exchange:   exchange,
		routingKey: routingKey,
		client:     client,
	}

	for _, option := range options {
		option(p)
	}

	return p
}

//
// WithProducerTemplate
// @desc 配置消息模板
// @param template amqp.Publishing
// @return ProducerOption
//
func WithProducerTemplate(template amqp.Publishing) ProducerOption {
	return func(p *Producer) {
		p.template = template
	}
}

//
// WithProducerMandatory
// @desc 配置 MANDATORY
// @param mandatory bool
// @return ProducerOption
//
func WithProducerMandatory(mandatory bool) ProducerOption {
	return func(p *Producer) {
		p.mandatory = mandatory
	}
}

//
// WithProducerImmediate
// @desc 配置 IMMEDIATE
// @param immediate bool
// @return ProducerOption
//
func WithProducerImmediate(immediate bool) ProducerOption {
	return func(p *Producer) {
		p.immediate = immediate
	}
}
