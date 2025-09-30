package rabbitmq

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Subscriber struct {
	rmq *RabbitMQ
}

type Retry struct {
	MaxRetry int
	Delay    time.Duration // thêm: delay giữa các lần retry
}

type OnFailedStrategy struct {
	Retry              *Retry
	HandleFailed       HandleFailedMessageFunc
	DeadLetterExchange string
}

type ProcessMessageFunc func(amqp.Delivery) error
type HandleFailedMessageFunc func(amqp.Delivery, error) error

func NewSubscriber(rmq *RabbitMQ) *Subscriber {
	return &Subscriber{rmq: rmq}
}

func (s *Subscriber) Subscribe(
	ctx context.Context,
	exc *ExchangeConfig,
	qc *QueueConfig,
	cf *ConsumerConfig,
	onFailed *OnFailedStrategy,
	process ProcessMessageFunc,
) error {
	if !s.rmq.Enable {
		return nil
	}

	ch, err := s.rmq.Conn.Channel()
	if err != nil {
		return fmt.Errorf("channel open failed: %w", err)
	}
	defer func(ch *amqp.Channel) {
		errClose := ch.Close()
		if errClose != nil {
			fmt.Println("could not close channel.got error:", errClose)
		}
	}(ch)

	// declare exchange/queue 1 lần khi subscribe
	if err = ch.ExchangeDeclare(exc.Name, exc.Type, exc.Durable, exc.AutoDelete, exc.Internal, exc.NoWait, exc.Args); err != nil {
		return fmt.Errorf("exchange declare failed: %w", err)
	}
	q, err := ch.QueueDeclare(qc.Name, qc.Durable, qc.AutoDelete, qc.Exclusive, qc.NoWait, qc.Args)
	if err != nil {
		return fmt.Errorf("queue declare failed: %w", err)
	}

	msgs, err := ch.Consume(q.Name, cf.Name, cf.AutoAck, cf.Exclusive, cf.NoLocal, cf.NoWait, cf.Args)
	if err != nil {
		return fmt.Errorf("consume failed: %w", err)
	}

	go s.consumeLoop(ctx, msgs, ch, onFailed, process)

	<-ctx.Done() // hủy theo context
	return nil
}

func (s *Subscriber) consumeLoop(
	ctx context.Context,
	msgs <-chan amqp.Delivery,
	ch *amqp.Channel,
	onFailed *OnFailedStrategy,
	process ProcessMessageFunc,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			if err := process(msg); err != nil {
				s.handleFailure(ctx, ch, msg, err, onFailed)
				continue
			}
			msg.Ack(false)
		}
	}
}

func (s *Subscriber) handleFailure(
	ctx context.Context,
	ch *amqp.Channel,
	msg amqp.Delivery,
	err error,
	onFailed *OnFailedStrategy,
) {
	if onFailed == nil {
		errNack := msg.Nack(false, false)
		if errNack != nil {
			return
		}
		return
	}

	// Retry logic
	if onFailed.Retry != nil {
		for i := 1; i <= onFailed.Retry.MaxRetry; i++ {
			time.Sleep(onFailed.Retry.Delay)
			if e := onFailed.HandleFailed(msg, err); e == nil {
				errAck := msg.Ack(false)
				if errAck != nil {
					return
				}
				return
			}
		}
	}

	// Dead letter
	if onFailed.DeadLetterExchange != "" {
		_ = ch.PublishWithContext(ctx, onFailed.DeadLetterExchange, msg.RoutingKey, false, false,
			amqp.Publishing{Body: msg.Body, Headers: msg.Headers})
	}
	errNack := msg.Nack(false, false)
	if errNack != nil {
		return
	}
}
