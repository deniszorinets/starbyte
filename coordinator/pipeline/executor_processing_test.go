package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"starbyte.io/coordinator/db"
	"starbyte.io/core/rpc"
)

var pipeline = db.Pipeline{
	Name: "test_pipeline",
	Steps: map[string]db.Step{
		"input": {Input: "", Id: inputUUID, Name: "input"},
		"step1": {Input: "input", Id: step1UUID, Name: "step1"},
		"step2": {Input: "step1", Id: step2UUID, Name: "step2"},
		"step3": {Input: "step2", Id: step3UUID, Name: "step3"},
		"step4": {Input: "input", Id: step4UUID, Name: "step4"},
		"step5": {Input: "step4", Id: step5UUID, Name: "step5"},
	},
}

func TestPumpBatchIntoInput(t *testing.T) {
	amqpMock := NewAmqpMock()
	dbRepo := RepositoryMock{}

	executor, err := NewExecutor(&pipeline, amqpMock, "https://test:test@test.com/test", &dbRepo)
	if err != nil {
		t.Errorf("executor creation error")
	}

	executor.pumpBatchIntoInput(context.TODO(), executor.inputRpcChannel)

	if len(amqpMock.Msgs) <= 0 {
		t.Errorf("expected message to be sent")
	}
}

func TestReadRespEventsSuccess(t *testing.T) {
	amqpMock := NewAmqpMock()

	executor, err := NewExecutor(&pipeline, amqpMock, "https://test:test@test.com/test", nil)
	if err != nil {
		t.Errorf("executor creation error")
	}

	ch, _ := executor.readRespEvents(context.TODO(), "")
	totalMessages := 0
	for msg := range ch {
		totalMessages++
		if msg.AmqpMessage == nil {
			t.Error("message not parsed correctly")
		}
		switch msg.Status {
		case rpc.ALLDONE:
			if msg.Error != "" {
				t.Error("error is not expected")
			}

		case rpc.ERROR:
			if msg.Error == "" {
				t.Error("error is expected")
			}

		case rpc.OK:
			if msg.AmqpMessage == nil {
				t.Error("message not parsed correctly")
			}

		}
	}

	if totalMessages != 3 {
		t.Error("expected 3 messages")
	}
}

func TestReadRespEventsQueueError(t *testing.T) {
	amqpMock := NewAmqpErrorMock()

	executor, err := NewExecutor(&pipeline, amqpMock, "https://test:test@test.com/test", nil)
	if err != nil {
		t.Errorf("executor creation error")
	}

	_, err = executor.readRespEvents(context.TODO(), "")

	if err == nil {
		t.Error("error expected")
	}
}

func TestReadRespEventsQueueDecodeError(t *testing.T) {
	amqpMock := NewAmqpMessageErrorMock()

	executor, err := NewExecutor(&pipeline, amqpMock, "https://test:test@test.com/test", nil)
	if err != nil {
		t.Errorf("executor creation error")
	}

	ch, _ := executor.readRespEvents(context.TODO(), "")
	totalMessages := 0
	for msg := range ch {
		totalMessages++
		m, _ := msg.AmqpMessage.(*AmqpMessageMock)
		if !m.IsNack {
			t.Error("message Nack was not called")
		}
	}

	if totalMessages != 3 {
		t.Error("expected 3 messages")
	}
}

func TestProcessOkEvent(t *testing.T) {
	amqpMock := NewAmqpMessageErrorMock()

	executor, err := NewExecutor(&pipeline, amqpMock, "https://test:test@test.com/test", nil)
	if err != nil {
		t.Errorf("executor creation error")
	}

	result, err := executor.buildOkRequest(rpc.ProcessResponse{
		CorrelationId: uuid.New(),
		Status:        rpc.OK,
		ResultUri:     "test",
	})

	if err != nil {
		t.Errorf("failed with error %s", err)
	}

	if result.ResourceUri != "test" {
		t.Errorf("result uri is wrong")
	}
}

func TestProcessErrorEvent(t *testing.T) {
	amqpMock := NewAmqpMessageErrorMock()
	repositoryMock := &RepositoryMock{
		MaxRetries: 3,
		Retries:    0,
	}

	executor, err := NewExecutor(&pipeline, amqpMock, "https://test:test@test.com/test", repositoryMock)
	if err != nil {
		t.Errorf("executor creation error")
	}

	result, err := executor.buildErrorRequest(rpc.ProcessResponse{
		CorrelationId: uuid.New(),
		Status:        rpc.ERROR,
		ResultUri:     "",
	})

	if err != nil {
		t.Errorf("failed with error %s", err)
	}

	if result.ResourceUri != "test_uri" {
		t.Errorf("result uri is wrong")
	}
}

func TestProcessErrorEventMaxRetries(t *testing.T) {
	amqpMock := NewAmqpMessageErrorMock()
	repositoryMock := &RepositoryMock{
		MaxRetries: 3,
		Retries:    3,
	}

	executor, err := NewExecutor(&pipeline, amqpMock, "https://test:test@test.com/test", repositoryMock)
	if err != nil {
		t.Errorf("executor creation error")
	}

	result, err := executor.buildErrorRequest(rpc.ProcessResponse{
		CorrelationId: uuid.New(),
		Status:        rpc.ERROR,
		ResultUri:     "",
	})

	if err != nil {
		t.Errorf("failed with error %s", err)
	}

	if result != nil {
		t.Errorf("result must be nil")
	}
}

func TestProcessErrorEventDbError(t *testing.T) {
	amqpMock := NewAmqpMessageErrorMock()
	repositoryMock := &RepositoryErrorMock{}

	executor, err := NewExecutor(&pipeline, amqpMock, "https://test:test@test.com/test", repositoryMock)
	if err != nil {
		t.Errorf("executor creation error")
	}

	_, err = executor.buildErrorRequest(rpc.ProcessResponse{
		CorrelationId: uuid.New(),
		Status:        rpc.ERROR,
		ResultUri:     "",
	})

	if !errors.Is(err, ErrRetries) {
		t.Errorf("error expected")
	}
}

func TestProcessErrorEventDbResourceUriError(t *testing.T) {
	amqpMock := NewAmqpMessageErrorMock()
	repositoryMock := &RepositoryResourceErrorMock{}

	executor, err := NewExecutor(&pipeline, amqpMock, "https://test:test@test.com/test", repositoryMock)
	if err != nil {
		t.Errorf("executor creation error")
	}

	_, err = executor.buildErrorRequest(rpc.ProcessResponse{
		CorrelationId: uuid.New(),
		Status:        rpc.ERROR,
		ResultUri:     "",
	})

	if !errors.Is(err, ErrResourceUri) {
		t.Errorf("error expected")
	}
}

func TestProcessInput(t *testing.T) {
	amqpMock := NewInputAmqpMock()
	repositoryMock := &RepositoryMock{MaxRetries: 3, Retries: 0}

	executor, err := NewExecutor(&pipeline, amqpMock, "https://test:test@test.com/test", repositoryMock)
	if err != nil {
		t.Errorf("executor creation error")
	}

	err = executor.executeInputProcessing(context.TODO(), executor.inputRpcChannel)

	if err != nil {
		t.Errorf("failed with error %s", err)
	}

	if len(amqpMock.Published) != 3 {
		t.Errorf("wrong queues amount")
	}

	for k, v := range amqpMock.Published {
		if strings.Contains(k, "input") {
			if len(v) != 4 {
				t.Errorf("wrong number of input request messages")
			}
			continue
		}
		if len(v) != 3 {
			t.Errorf("wrong number of step request messages")
		}
	}
}

func TestProcessStep(t *testing.T) {
	amqpMock := NewInputAmqpMock()
	repositoryMock := &RepositoryMock{MaxRetries: 3, Retries: 0}

	executor, err := NewExecutor(&pipeline, amqpMock, "https://test:test@test.com/test", repositoryMock)
	if err != nil {
		t.Errorf("executor creation error")
	}

	err = executor.executeNodeProcessing(context.TODO(), executor.inputRpcChannel.next[0])

	if err != nil {
		t.Errorf("failed with error %s", err)
	}

	if len(amqpMock.Published) != 1 {
		t.Errorf("wrong queues amount")
	}

	for _, v := range amqpMock.Published {
		if len(v) != 3 {
			t.Errorf("wrong number of input request messages")
		}

	}
}
