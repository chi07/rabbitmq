package rabbitmq

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

type Publisher struct {
	rmq *RabbitMQ
}

func NewPublisher(rmq *RabbitMQ) *Publisher {
	return &Publisher{
		rmq: rmq,
	}
}

func (p *Publisher) Publish(exc *ExchangeConfig, qc *QueueConfig, routingKey string, msg *Message) error {
	// Checking if RabbitMQ is enabled
	if !p.rmq.Enable {
		return nil
	}

	// Open a channel
	ch, err := p.rmq.Conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declare a topic exchange
	err = ch.ExchangeDeclare(
		exc.Name,       // exchange name
		exc.Type,       // exchange type
		exc.Durable,    // durable
		exc.AutoDelete, // auto-deleted
		exc.Internal,   // internal
		exc.NoWait,     // no-wait
		exc.Args,       // arguments
	)
	failOnError(err, "Failed to ExchangeDeclare a queue")

	// Declare a queue
	q, err := ch.QueueDeclare(
		qc.Name,       // queue name (auto-generated)
		qc.Durable,    // durable
		qc.AutoDelete, // delete when unused
		qc.Exclusive,  // exclusive
		qc.NoWait,     // no-wait
		qc.Args,       // arguments
	)

	failOnError(err, "Failed to QueueDeclare a queue")

	// Bind the queue to the exchange
	err = ch.QueueBind(
		q.Name,     // queue name
		routingKey, // routing key
		exc.Name,   // exchange name
		exc.NoWait,
		exc.Args,
	)
	failOnError(err, "Failed to bind a message")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx, exc.Name, routingKey, msg.Mandatory, msg.Immediate, amqp.Publishing{
		ContentType: msg.ContentType,
		Body:        msg.Body,
	})
	failOnError(err, "Failed to publish a message")

	return nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
	return
}
