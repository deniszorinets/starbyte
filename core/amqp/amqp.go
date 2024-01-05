package amqp

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"starbyte.io/core/utils"
)

const RABBITMQ_QUEUE_EXPIRATION_TTL = 100000 // 100 seconds
const RABBITMQ_QUEUE_MAX_LENGTH = 100

type AmqpMessage interface {
	Ack(multiple bool) error
	Nack(multiple bool, requeue bool) error
	Reject(requeue bool) error
	Payload() []byte
}

type AmqpMessageRabbitImpl struct {
	*amqp.Delivery
}

func (m *AmqpMessageRabbitImpl) Payload() []byte {
	return m.Body
}

type Amqp interface {
	Connect(context.Context, string) error
	QueueDeclare(string) error
	Close() error
	Messages(context.Context, string) (<-chan AmqpMessage, error)
	Publish(context.Context, string, any) error
}

type RabbitMqAmqp struct {
	amqpConnection *amqp.Connection
}

func (broker *RabbitMqAmqp) Connect(ctx context.Context, uri string) error {
	conn, err := amqp.Dial(uri)

	if err != nil {
		return fmt.Errorf("can not connect to amqp broker: %e", err)
	}

	broker.amqpConnection = conn

	return nil
}

func (broker *RabbitMqAmqp) QueueDeclare(queueName string) error {
	ch, err := broker.amqpConnection.Channel()

	if err != nil {
		return fmt.Errorf("can not open amqp channel: %e", err)
	}

	_, err = ch.QueueDeclare(queueName, true, false, false, false, amqp.Table{
		"x-expires": RABBITMQ_QUEUE_EXPIRATION_TTL,
	})

	if err != nil {
		return err
	}

	_, err = ch.QueuePurge(queueName, false)

	return err
}

func (broker *RabbitMqAmqp) Close() error {
	if broker.amqpConnection != nil {
		err := broker.amqpConnection.Close()

		if err != nil {
			return fmt.Errorf("can not close amqp broker connection: %e", err)
		}
	}

	return nil
}

func (broker *RabbitMqAmqp) Messages(ctx context.Context, queueName string) (<-chan AmqpMessage, error) {
	ch, err := broker.amqpConnection.Channel()

	if err != nil {
		return nil, fmt.Errorf("can not open amqp channel: %e", err)
	}
	msgs, err := ch.ConsumeWithContext(
		ctx,
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("can not consume messsages from channel: %e", err)
	}

	return utils.CastChannelWithContext[AmqpMessage, amqp.Delivery](ctx, msgs, func(record amqp.Delivery) AmqpMessage { return &AmqpMessageRabbitImpl{&record} }), nil
}

func (broker *RabbitMqAmqp) Publish(ctx context.Context, queueName string, message any) error {
	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal response")
	}

	ch, err := broker.amqpConnection.Channel()

	if err != nil {
		return fmt.Errorf("can not open amqp channel: %e", err)
	}

	err = ch.PublishWithContext(ctx,
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		})

	if err != nil {
		return fmt.Errorf("failed to publish message to queue %e", err)
	}
	return nil
}
