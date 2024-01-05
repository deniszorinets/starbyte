package worker

import (
	"context"
	"fmt"
	"sync"

	"starbyte.io/core/rpc"
	"starbyte.io/core/utils"
	"starbyte.io/sdk"
)

type StepWorker struct {
	*BaseWorker
	step sdk.Step
}

func NewStepWorker(step sdk.Step) (*StepWorker, error) {
	worker, err := NewWorker()
	if err != nil {
		return nil, err
	}

	return &StepWorker{
		BaseWorker: worker,
		step:       step,
	}, nil
}

func process(ctx context.Context, input <-chan any, results chan<- any, step sdk.Step) {
	defer close(results)
	ctx, cancel := context.WithCancelCause(ctx)

	for data := range input {
		if context.Cause(ctx) != nil {
			break
		}
		result, err := step.Transform(data)
		if err != nil {
			cancel(err)
		}
		results <- result
	}
}

func (worker *StepWorker) processMessages(ctx context.Context, messages <-chan rpc.ProcessRequest) {
	for message := range messages {
		var err error = nil

		input, err := worker.ReadInputFile(ctx, message.ResourceUri)

		results := make(chan any, 1)

		if before, ok := worker.step.(sdk.StepBefore); ok {
			err = before.BeforeTransform()
		}

		if err == nil {
			wg := &sync.WaitGroup{}

			utils.RunInWg(wg, func() { process(ctx, input, results, worker.step) })
			utils.RunInWg(wg, func() { err = worker.WriteOutput(ctx, results, message.ResultUri) })

			wg.Wait()
		}

		if after, ok := worker.step.(sdk.StepAfter); ok {
			err = after.AfterTransform()
		}
		if err != nil || context.Cause(ctx) != nil {
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

func (worker *StepWorker) Run() error {
	ctx := context.Background()
	err := worker.Listen(ctx)
	defer worker.Close()
	if err != nil {
		return err
	}

	messagesCh, err := worker.Messages(ctx)
	if err != nil {
		return err
	}

	worker.processMessages(ctx, messagesCh)

	return context.Cause(ctx)
}
