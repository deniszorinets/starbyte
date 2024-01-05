package worker

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"starbyte.io/core/rpc"
	s3io "starbyte.io/core/s3"
	"starbyte.io/core/utils"
	"starbyte.io/sdk"
)

const (
	LIGHTBYTE_WORKER_BATCH_SIZE    = "LIGHTBYTE_WORKER_BATCH_SIZE"
	LIGHTBYTE_WORKER_BATCH_TIMEOUT = "LIGHTBYTE_WORKER_BATCH_TIMEOUT"
)

type InputWorker struct {
	*BaseWorker
	input        sdk.Input
	batchSize    int
	batchTimeout int
}

func NewInputWorker(input sdk.Input) (*InputWorker, error) {
	worker, err := NewWorker()
	if err != nil {
		return nil, err
	}

	batchSize, err := strconv.Atoi(os.Getenv(LIGHTBYTE_WORKER_BATCH_SIZE))
	if err != nil {
		return nil, err
	}

	batchTimeout, err := strconv.Atoi(os.Getenv(LIGHTBYTE_WORKER_BATCH_TIMEOUT))
	if err != nil {
		return nil, err
	}

	return &InputWorker{
		BaseWorker:   worker,
		input:        input,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
	}, nil
}

func writeOutput(ctx context.Context, batch Batch, fileUri string) error {
	out, in := io.Pipe()
	writeWg := sync.WaitGroup{}

	ctx, cancel := context.WithCancelCause(ctx)
	writeWg.Add(2)
	go func() {
		defer in.Close()
		defer writeWg.Done()

		cborWriter, err := s3io.NewWriter(in)

		if err != nil {
			cancel(err)
		}

		for _, rec := range batch.Data {
			cborWriter.Write(rec)
		}

		cborWriter.Close()
	}()

	go func() {
		defer out.Close()
		defer writeWg.Done()

		err := s3io.Write(ctx, fileUri, out)
		if err != nil {
			cancel(err)
		}
	}()
	writeWg.Wait()

	return context.Cause(ctx)
}

func (worker *InputWorker) processMessages(ctx context.Context, messages <-chan rpc.ProcessRequest, batches <-chan Batch) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for message := range messages {
		batch, ok := <-batches
		if !ok {
			message.Ack(false)
			worker.Publish(ctx, rpc.ProcessResponse{
				CorrelationId: message.CorrelationId,
				Status:        rpc.ALLDONE,
			})
			cancel()
			break
		}
		err := writeOutput(ctx, batch, message.ResultUri)
		if err != nil {
			message.Ack(false)
			worker.Publish(ctx, rpc.ProcessResponse{
				CorrelationId: message.CorrelationId,
				Status:        rpc.ERROR,
				Error:         fmt.Sprintf("%e", err),
			})
		} else {
			message.Ack(false)
			worker.Publish(ctx, rpc.ProcessResponse{
				CorrelationId: message.CorrelationId,
				ResultUri:     message.ResultUri,
				Status:        rpc.OK,
			})
		}
	}
}

func (worker *InputWorker) Run() error {
	ctx := context.Background()
	processCtx := context.Background()

	err := worker.Listen(ctx)
	defer worker.Close()
	if err != nil {
		return err
	}

	messagesCh, err := worker.Messages(ctx)
	if err != nil {
		return err
	}

	rawInput := make(chan any, 1)
	batchCh := make(chan Batch, 1)

	wg := &sync.WaitGroup{}

	utils.RunInWg(wg, func() { worker.input.Read(processCtx, rawInput) })
	utils.RunInWg(wg, func() { ChunkToBatch(processCtx, rawInput, batchCh, worker.batchSize, worker.batchTimeout) })
	utils.RunInWg(wg, func() { worker.processMessages(ctx, messagesCh, batchCh) })

	wg.Wait()

	return context.Cause(processCtx)
}
