package rabbitmq

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Subscriber struct {
	rmq *RabbitMQ
}

type Retry struct {
	MaxRetry int
}

type OnFailedStrategy struct {
	Retry                   *Retry
	handleFailedMessageFunc *HandleFailedMessageFunc
	deadLetterExchange      string
}

func NewOnFailedStrategy(retry *Retry, handleFailedMessageFunc *HandleFailedMessageFunc, deadLetterExchange string) *OnFailedStrategy {
	return &OnFailedStrategy{
		Retry:                   retry,
		handleFailedMessageFunc: handleFailedMessageFunc,
		deadLetterExchange:      deadLetterExchange,
	}
}

func NewSubscriber(rmq *RabbitMQ) *Subscriber {
	return &Subscriber{
		rmq: rmq,
	}
}

type ProcessMessageFunc func(message amqp.Delivery) error
type HandleFailedMessageFunc func(message amqp.Delivery, err error) error

func (s *Subscriber) Subscribe(
	exc *ExchangeConfig,
	queueName string,
	cf *ConsumerConfig,
	onFailedStrategy *OnFailedStrategy,
	processMessageFunc ProcessMessageFunc,
	handleFailedMessageFunc HandleFailedMessageFunc) error {
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
		queueName,      // queue name (auto-generated)
		exc.Durable,    // durable
		exc.AutoDelete, // delete when unused
		exc.Exclusive,  // exclusive
		exc.NoWait,     // no-wait
		exc.Args,       // arguments
	)

	if err != nil {
		return fmt.Errorf("failed to QueueDeclare a queue: %s got error: %w", queueName, err)
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
		consumeMessage(messages, ch, onFailedStrategy, processMessageFunc, handleFailedMessageFunc)
	}()

	log.Printf("[*] Waiting for messages.")
	<-forever
	return nil
}

func consumeMessage(messages <-chan amqp.Delivery, ch *amqp.Channel, onFailedStrategy *OnFailedStrategy, processMessage ProcessMessageFunc, handleFailedMessageFunc HandleFailedMessageFunc) {
	for message := range messages {
		err := processMessage(message)
		if err != nil {
			// Handle the error and retry
			if onFailedStrategy != nil && onFailedStrategy.Retry != nil && onFailedStrategy.Retry.MaxRetry > 0 {
				retryCount := 0
				maxRetries := onFailedStrategy.Retry.MaxRetry

				for retryCount < maxRetries {
					retryCount++
					time.Sleep(time.Duration(retryCount) * time.Second)

					err = processMessage(message)
					if err == nil {
						// Message processed successfully
						message.Ack(false)
						break
					}
				}
			}

			if err != nil {
				// Failed to process the message after retries
				err = handleFailedMessageFunc(message, err)
				if err != nil {
					log.Printf("Failed to handle failed message: %s", err.Error())
					continue
				}
				if onFailedStrategy != nil && onFailedStrategy.deadLetterExchange != "" {
					moveToDeadLetterQueue(message, ch, onFailedStrategy.deadLetterExchange)
				}
				continue
			}
		}

		// Message processed successfully
		message.Ack(false)
	}
}

func moveToDeadLetterQueue(message amqp.Delivery, ch *amqp.Channel, dlqName string) {
	// Re-declare the Dead Letter Queue
	_, err := ch.QueueDeclare(
		dlqName, // dead letter queue name
		true,    // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		log.Printf("Failed to declare DLQ: %s", err.Error())
		return
	}

	// Create a new headers table for the message
	headers := make(amqp.Table)
	headers["x-original-exchange"] = message.Exchange
	headers["x-original-routing-key"] = message.RoutingKey

	// Publish the message to the Dead Letter Queue with the original headers
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = ch.PublishWithContext(
		ctx,
		"",      // exchange
		dlqName, // routing key
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			Headers: headers,
			Body:    message.Body,
		},
	)
	if err != nil {
		log.Printf("Failed to move message %v to Dead Letter Queue: %s got error %s", message, dlqName, err.Error())
		return
	}

	log.Printf("Message %v moved to Dead Letter Queue: %s", message, string(message.Body))
}
