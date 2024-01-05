package pipeline

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"starbyte.io/core/amqp"
	"starbyte.io/core/rpc"
)

type AmqpMock struct {
	Queues []string
	Msgs   []any
}

type AmqpMessageMock struct {
	Body   []byte
	IsAck  bool
	IsNack bool
}

func (m *AmqpMessageMock) Ack(bool) error {
	m.IsAck = true
	return nil
}

func (m *AmqpMessageMock) Nack(bool, bool) error {
	m.IsNack = true
	return nil
}

func (m *AmqpMessageMock) Payload() []byte {
	return m.Body
}

func (m *AmqpMessageMock) Reject(bool) error {
	return nil
}

func NewAmqpMock() *AmqpMock {
	return &AmqpMock{
		Queues: []string{},
		Msgs:   []any{},
	}
}

func (*AmqpMock) Connect(ctx context.Context, uri string) error {
	return nil
}

func (m *AmqpMock) QueueDeclare(queueName string) error {
	m.Queues = append(m.Queues, queueName)
	return nil
}

func (*AmqpMock) Close() error {
	return nil
}

func (*AmqpMock) Messages(ctx context.Context, queueName string) (<-chan amqp.AmqpMessage, error) {
	resp1 := rpc.ProcessResponse{
		CorrelationId: uuid.New(),
		Status:        rpc.ALLDONE,
		ResultUri:     "test",
	}

	resp2 := rpc.ProcessResponse{
		CorrelationId: uuid.New(),
		Status:        rpc.ERROR,
		Error:         "error",
	}

	resp3 := rpc.ProcessResponse{
		CorrelationId: uuid.New(),
		Status:        rpc.OK,
		ResultUri:     "test",
	}

	resp1Bytes, _ := json.Marshal(resp1)
	resp2Bytes, _ := json.Marshal(resp2)
	resp3Bytes, _ := json.Marshal(resp3)

	messages := []amqp.AmqpMessage{}
	messages = append(
		messages,
		&AmqpMessageMock{Body: resp1Bytes},
		&AmqpMessageMock{Body: resp2Bytes},
		&AmqpMessageMock{Body: resp3Bytes},
	)

	result := make(chan amqp.AmqpMessage, 1)

	go func() {
		defer close(result)
		for _, m := range messages {
			result <- m
		}
	}()
	return result, nil
}

func (m *AmqpMock) Publish(ctx context.Context, queueName string, message any) error {
	m.Msgs = append(m.Msgs, message)
	return nil
}

var ErrAmqpTest = errors.New("amqp test mock error")

type AmqpErrorMock struct {
}

func NewAmqpErrorMock() *AmqpErrorMock {
	return &AmqpErrorMock{}
}

func (*AmqpErrorMock) Connect(ctx context.Context, uri string) error {
	return ErrAmqpTest
}

func (m *AmqpErrorMock) QueueDeclare(queueName string) error {
	return ErrAmqpTest
}

func (*AmqpErrorMock) Close() error {
	return ErrAmqpTest
}

func (*AmqpErrorMock) Messages(ctx context.Context, queueName string) (<-chan amqp.AmqpMessage, error) {
	return nil, ErrAmqpTest
}

func (*AmqpErrorMock) Publish(ctx context.Context, queueName string, message any) error {
	return ErrAmqpTest
}

type AmqpMessageErrorMock struct {
	AmqpErrorMock
}

func NewAmqpMessageErrorMock() *AmqpMessageErrorMock {
	return &AmqpMessageErrorMock{}
}

func (*AmqpMessageErrorMock) Messages(ctx context.Context, queueName string) (<-chan amqp.AmqpMessage, error) {

	messages := []amqp.AmqpMessage{}
	messages = append(
		messages,
		&AmqpMessageMock{Body: []byte("not a json 1")},
		&AmqpMessageMock{Body: []byte("not a json 2")},
		&AmqpMessageMock{Body: []byte("not a json 3")},
	)

	result := make(chan amqp.AmqpMessage, 1)

	go func() {
		defer close(result)
		for _, m := range messages {
			result <- m
		}
	}()
	return result, nil
}

type InputAmqpMock struct {
	Queues    []string
	Published map[string][]any
}

func NewInputAmqpMock() *InputAmqpMock {
	return &InputAmqpMock{
		Published: map[string][]any{},
	}
}

func (*InputAmqpMock) Connect(ctx context.Context, uri string) error {
	return nil
}

func (m *InputAmqpMock) QueueDeclare(queueName string) error {
	m.Queues = append(m.Queues, queueName)
	return nil
}

func (*InputAmqpMock) Close() error {
	return nil
}

func (*InputAmqpMock) Messages(ctx context.Context, queueName string) (<-chan amqp.AmqpMessage, error) {
	resp1 := rpc.ProcessResponse{
		CorrelationId: uuid.New(),
		Status:        rpc.OK,
		ResultUri:     "test1",
	}

	resp2 := rpc.ProcessResponse{
		CorrelationId: uuid.New(),
		Status:        rpc.OK,
		ResultUri:     "test2",
	}

	resp3 := rpc.ProcessResponse{
		CorrelationId: uuid.New(),
		Status:        rpc.OK,
		ResultUri:     "test3",
	}

	resp4 := rpc.ProcessResponse{
		CorrelationId: uuid.New(),
		Status:        rpc.ALLDONE,
	}

	resp1Bytes, _ := json.Marshal(resp1)
	resp2Bytes, _ := json.Marshal(resp2)
	resp3Bytes, _ := json.Marshal(resp3)
	resp4Bytes, _ := json.Marshal(resp4)

	messages := []amqp.AmqpMessage{}
	messages = append(
		messages,
		&AmqpMessageMock{Body: resp1Bytes},
		&AmqpMessageMock{Body: resp2Bytes},
		&AmqpMessageMock{Body: resp3Bytes},
		&AmqpMessageMock{Body: resp4Bytes},
	)

	result := make(chan amqp.AmqpMessage, 1)

	go func() {
		defer close(result)
		for _, m := range messages {
			result <- m
		}
	}()
	return result, nil
}

func (m *InputAmqpMock) Publish(ctx context.Context, queueName string, message any) error {
	if _, ok := m.Published[queueName]; !ok {
		m.Published[queueName] = []any{}
	}
	m.Published[queueName] = append(m.Published[queueName], message)
	return nil
}
