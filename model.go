package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	Enable bool
	URL    string
	Conn   *amqp.Connection
}

type Message struct {
	Body        []byte
	Mandatory   bool
	Immediate   bool
	ContentType string
}

type ExchangeConfig struct {
	Name       string
	Type       string
	Durable    bool
	Internal   bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type QueueConfig struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type ConsumerConfig struct {
	Name      string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

func NewRabbitMQ(dns string, enable bool) *RabbitMQ {
	return &RabbitMQ{
		URL:    dns,
		Enable: enable,
	}
}

func (mq *RabbitMQ) Connect() (*amqp.Connection, error) {
	conn, err := amqp.Dial(mq.URL)
	mq.Conn = conn
	return conn, err
}
