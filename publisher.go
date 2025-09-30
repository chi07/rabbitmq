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
	return &Publisher{rmq: rmq}
}

func (p *Publisher) Publish(
	ctx context.Context,
	exchangeName, routingKey string,
	msg amqp.Publishing,
) error {
	if !p.rmq.Enable {
		return nil
	}

	ch, err := p.rmq.Conn.Channel()
	if err != nil {
		return fmt.Errorf("channel open failed: %w", err)
	}
	defer func(ch *amqp.Channel) {
		errClose := ch.Close()
		if errClose != nil {
			fmt.Println("could not close channel.got error:", errClose)
		}
	}(ch)

	// fallback timeout nếu ctx chưa có deadline
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}

	if err := ch.PublishWithContext(ctx, exchangeName, routingKey, false, false, msg); err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}
	return nil
}
