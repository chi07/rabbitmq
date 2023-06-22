package rabbitmq

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

type Subscriber struct {
	rmq *RabbitMQ
}

func NewSubscriber(rmq *RabbitMQ) *Subscriber {
	return &Subscriber{
		rmq: rmq,
	}
}

type HandleMessageFunc func(message []byte) error

func (s *Subscriber) Subscribe(exc *ExchangeConfig, qc *QueueConfig, cf *ConsumerConfig, handleMsgFunc HandleMessageFunc) error {
	// Checking if RabbitMQ is enabled
	if !s.rmq.Enable {
		return nil
	}

	// Open a channel
	ch, err := s.rmq.Conn.Channel()
	defer func() {
		if err := ch.Close(); err != nil {
			fmt.Println("could not close channel.got error:", err)
		}
	}()

	defer ch.Close()

	// Declare a topic exchange
	if err = ch.ExchangeDeclare(
		exc.Name,       // exchange name
		exc.Type,       // exchange type
		exc.Durable,    // durable
		exc.AutoDelete, // auto-deleted
		exc.Internal,   // internal
		exc.NoWait,     // no-wait
		exc.Args,       // arguments
	); err != nil {
		return fmt.Errorf("failed to ExchangeDeclare a queue: %s got error: %w", exc.Name, err)
	}

	// Declare a queue
	q, err := ch.QueueDeclare(
		qc.Name,       // queue name (auto-generated)
		qc.Durable,    // durable
		qc.AutoDelete, // delete when unused
		qc.Exclusive,  // exclusive
		qc.NoWait,     // no-wait
		qc.Args,       // arguments
	)

	if err != nil {
		return fmt.Errorf("failed to QueueDeclare a queue: %s got error: %w", qc.Name, err)
	}

	// start consuming messages
	messages, err := ch.Consume(
		q.Name,       // queue
		cf.Name,      // consumer
		cf.AutoAck,   // auto-ack
		cf.Exclusive, // exclusive
		cf.NoLocal,   // no-local
		cf.NoWait,    // no-wait
		cf.Args,      // args
	)

	if err != nil {
		return fmt.Errorf("failed to Consume a message on queue: %s got error: %w", q.Name, err)
	}

	var forever chan struct{}

	go func() {
		for msg := range messages {
			log.Printf("Received a message: %s", msg.Body)
			if err := handleMsgFunc(msg.Body); err != nil {
				log.Error("failed to handle message: %s", err)
			}
		}
	}()

	log.Printf("[*] Waiting for messages.")
	<-forever
	return nil
}
