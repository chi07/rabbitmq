package rabbitmq

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	rmq *RabbitMQ
}

func NewPublisher(rmq *RabbitMQ) *Publisher {
	return &Publisher{
		rmq: rmq,
	}
}

func (p *Publisher) Publish(exc *ExchangeConfig, qc *QueueConfig, routingKey string, msg *Message, publishing amqp.Publishing) error {
	// Checking if RabbitMQ is enabled
	if !p.rmq.Enable {
		return nil
	}

	// Open a channel
	ch, err := p.rmq.Conn.Channel()
	if err != nil {
		return err
	}

	defer func() {
		if err := ch.Close(); err != nil {
			fmt.Println("could not close channel.got error:", err)
		}
	}()

	// Declare a topic exchange
	if err := ch.ExchangeDeclare(
		exc.Name,       // exchange name
		exc.Type,       // exchange type
		exc.Durable,    // durable
		exc.AutoDelete, // auto-deleted
		exc.Internal,   // internal
		exc.NoWait,     // no-wait
		exc.Args,       // arguments
	); err != nil {
		return fmt.Errorf("failed to ExchangeDeclare a queue: %w", err)
	}

	// Declare a queue
	q, err := ch.QueueDeclare(
		qc.Name,       // queue name
		qc.Durable,    // durable
		qc.AutoDelete, // delete when unused
		qc.Exclusive,  // exclusive
		qc.NoWait,     // no-wait
		qc.Args,       // arguments
	)

	if err != nil {
		return fmt.Errorf("failed to ExchangeDeclare a queue: %s %w", qc.Name, err)
	}

	// Bind the queue to the exchange
	if err := ch.QueueBind(
		q.Name,     // queue name
		routingKey, // routing key
		exc.Name,   // exchange name
		exc.NoWait,
		exc.Args,
	); err != nil {
		return fmt.Errorf("failed to QueueBind a queue: %s %w", q.Name, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err = ch.PublishWithContext(ctx, exc.Name, routingKey, msg.Mandatory, msg.Immediate, publishing); err != nil {
		return fmt.Errorf("failed to publish a message: %w", err)
	}

	return nil
}
