package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"

	"starbyte.io/core/amqp"
	"starbyte.io/core/rpc"
	s3io "starbyte.io/core/s3"
	"starbyte.io/core/utils"
)

const (
	LIGHTBYTE_WORKER_AMQP_URI       = "LIGHTBYTE_WORKER_AMQP_URI"
	LIGHTBYTE_WORKER_LISTEN_QUEUE   = "LIGHTBYTE_WORKER_LISTEN_QUEUE"
	LIGHTBYTE_WORKER_RESPONSE_QUEUE = "LIGHTBYTE_WORKER_RESPONSE_QUEUE"
)

type BaseWorker struct {
	amqpUri       string
	listenQueue   string
	responseQueue string
	amqp          amqp.Amqp
}

type Worker interface {
	Run() error
}

func NewWorker() (*BaseWorker, error) {

	return &BaseWorker{
		amqp:          &amqp.RabbitMqAmqp{},
		amqpUri:       os.Getenv(LIGHTBYTE_WORKER_AMQP_URI),
		listenQueue:   os.Getenv(LIGHTBYTE_WORKER_LISTEN_QUEUE),
		responseQueue: os.Getenv(LIGHTBYTE_WORKER_RESPONSE_QUEUE),
	}, nil
}

func (worker *BaseWorker) Listen(ctx context.Context) error {
	return worker.amqp.Connect(ctx, worker.amqpUri)
}

func (worker *BaseWorker) Close() error {
	return worker.amqp.Close()
}

func (worker *BaseWorker) Messages(ctx context.Context) (chan rpc.ProcessRequest, error) {
	resultCh := make(chan rpc.ProcessRequest, 1)
	messages, err := worker.amqp.Messages(ctx, worker.listenQueue)

	_, cancel := context.WithCancelCause(ctx)
	if err != nil {
		return nil, err
	}

	go func() {
		for msg := range messages {
			var rpcMsg rpc.ProcessRequest
			err := json.Unmarshal(msg.Payload(), &rpcMsg)
			if err != nil {
				cancel(err)
				return
			}
			rpcMsg.AmqpMessage = msg
			resultCh <- rpcMsg
		}
	}()

	return resultCh, nil
}

func (worker *BaseWorker) Publish(ctx context.Context, resp rpc.ProcessResponse) error {
	return worker.amqp.Publish(ctx, worker.responseQueue, resp)
}

func (worker *BaseWorker) ReadInputFile(ctx context.Context, fileUri string) (chan any, error) {
	fileStream, err := s3io.Read(ctx, fileUri)

	if err != nil {
		return nil, err
	}

	cborReader, err := s3io.NewReader(fileStream)
	if err != nil {
		return nil, err
	}

	output := make(chan any, 1)

	go func() {
		defer close(output)
		for record, err := cborReader.Read(); !errors.Is(err, io.EOF); record, err = cborReader.Read() {
			if errors.Is(context.Cause(ctx), context.Canceled) {
				break
			}
			output <- record
		}
	}()

	return output, nil
}

func (worker *BaseWorker) WriteOutput(ctx context.Context, output chan any, fileUri string) error {
	out, in := io.Pipe()
	defer out.Close()

	go func() {
		defer in.Close()

		cborWriter, _ := s3io.NewWriter(in)

		for record := range utils.ReadChannelWithContext(ctx, output) {
			cborWriter.Write(record)
		}

		cborWriter.Close()
	}()

	return s3io.Write(ctx, fileUri, out)
}
