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
