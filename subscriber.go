package rabbitmq

import "log"

type Subscriber struct {
	rmq *RabbitMQ
}

func NewSubscriber(rmq *RabbitMQ) *Subscriber {
	return &Subscriber{
		rmq: rmq,
	}
}

type MessageHandler func(message []byte)

func (s *Subscriber) Subscribe(exc *ExchangeConfig, qc *QueueConfig, cf *ConsumerConfig, handler MessageHandler) error {
	// Checking if RabbitMQ is enabled
	if !s.rmq.Enable {
		return nil
	}

	// Open a channel
	ch, err := s.rmq.Conn.Channel()
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
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range messages {
			handler(d.Body)
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf("[*] Waiting for messages.")
	<-forever
	return nil
}
