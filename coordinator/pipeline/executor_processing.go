package pipeline

import (
	"context"
	"encoding/json"

	"starbyte.io/core/rpc"
)

func (exec *PipelineExecutor) pumpBatchIntoInput(ctx context.Context, node *amqpRpcPair) error {
	batchUri := exec.getNewResultUri()
	correlationId := exec.newCorrelationId()
	req := &rpc.ProcessRequest{
		ResultUri:     batchUri,
		CorrelationId: correlationId,
	}

	batch, err := exec.dbRepository.CreateNewBatch(nil, node.vertex.Step.Id, batchUri)
	if err != nil {
		return err
	}
	err = exec.dbRepository.CreateNewBatchProcessAttempt(batch.BatchId, correlationId)
	if err != nil {
		return err
	}

	return exec.amqpConn.Publish(ctx, node.req, req)
}

func (exec *PipelineExecutor) buildOkRequest(event rpc.ProcessResponse) (*rpc.ProcessRequest, error) {
	return &rpc.ProcessRequest{
		ResourceUri:   event.ResultUri,
		ResultUri:     exec.getNewResultUri(),
		CorrelationId: exec.newCorrelationId(),
	}, nil
}

func (exec *PipelineExecutor) buildErrorRequest(event rpc.ProcessResponse) (*rpc.ProcessRequest, error) {
	retries, err := exec.dbRepository.GetMaxRetriesByCorrelationId(event.CorrelationId)
	if err != nil {
		return nil, err
	}
	if retries.MaxRetries <= retries.Retries {
		return nil, nil
	} else {
		resourceUri, err := exec.dbRepository.GetResourceUriByCorrelationId(event.CorrelationId)
		if err != nil {
			return nil, err
		}
		return &rpc.ProcessRequest{
			ResourceUri:   resourceUri,
			ResultUri:     exec.getNewResultUri(),
			CorrelationId: exec.newCorrelationId(),
		}, nil
	}
}

func (exec *PipelineExecutor) processOkEvent(ctx context.Context, event rpc.ProcessResponse, node *amqpRpcPair) error {

	err := exec.dbRepository.SetBatchProcessStatus(event.CorrelationId, string(rpc.OK), "")
	if err != nil {
		return err
	}

	if node.next == nil {
		return nil
	}

	for _, next := range node.next {
		res, err := exec.buildOkRequest(event)
		if err != nil {
			return err
		}
		if res == nil {
			return nil
		}

		batch, err := exec.dbRepository.CreateNewBatch(&node.vertex.Step.Id, next.vertex.Step.Id, res.ResourceUri)
		if err != nil {
			return err
		}
		err = exec.dbRepository.CreateNewBatchProcessAttempt(batch.BatchId, res.CorrelationId)
		if err != nil {
			return err
		}

		err = exec.amqpConn.Publish(ctx, next.req, res)
		if err != nil {
			return err
		}
	}
	return nil
}

func (exec *PipelineExecutor) processErrorEvent(ctx context.Context, event rpc.ProcessResponse, node *amqpRpcPair) error {

	batch, err := exec.dbRepository.GetBatchByCorrelationId(event.CorrelationId)
	if err != nil {
		return err
	}

	err = exec.dbRepository.SetBatchProcessStatus(event.CorrelationId, string(rpc.ERROR), event.Error)
	if err != nil {
		return err
	}

	if node.next == nil {
		return nil
	}

	for _, next := range node.next {
		if next.vertex.Step.Id != batch.StepToId {
			continue
		}
		res, err := exec.buildErrorRequest(event)
		if err != nil {
			return err
		}
		if res == nil {
			return nil
		}
		err = exec.amqpConn.Publish(ctx, next.req, res)
		if err != nil {
			return err
		}

		err = exec.dbRepository.CreateNewBatchProcessAttempt(batch.BatchId, res.CorrelationId)

		if err != nil {
			return err
		}
	}
	return nil
}

func (exec *PipelineExecutor) processAllDoneEvent(ctx context.Context, event rpc.ProcessResponse, node *amqpRpcPair) error {
	return exec.dbRepository.DiscardBatchByCorrelationId(event.CorrelationId)
}

func (exec *PipelineExecutor) readRespEvents(ctx context.Context, queueName string) (chan rpc.ProcessResponse, error) {
	resultCh := make(chan rpc.ProcessResponse, 1)
	messages, err := exec.amqpConn.Messages(ctx, queueName)

	if err != nil {
		return nil, err
	}

	go func() {
		defer close(resultCh)
		for msg := range messages {
			var rpcMsg rpc.ProcessResponse
			err := json.Unmarshal(msg.Payload(), &rpcMsg)
			if err != nil {
				msg.Nack(false, false)
			}

			rpcMsg.AmqpMessage = msg

			resultCh <- rpcMsg
		}
	}()

	return resultCh, nil
}

func (exec *PipelineExecutor) executeNodeProcessing(ctx context.Context, node *amqpRpcPair) error {
	events, err := exec.readRespEvents(ctx, node.resp)

	if err != nil {
		return err
	}

	for event := range events {
		switch event.Status {
		case rpc.OK:
			if err := exec.processOkEvent(ctx, event, node); err != nil {
				return err
			}
		case rpc.ERROR:
			if err := exec.processErrorEvent(ctx, event, node); err != nil {
				return err
			}
		case rpc.ALLDONE:
			if err := exec.processAllDoneEvent(ctx, event, node); err != nil {
				return err
			}
		}
		err = event.Ack(false)
		if err != nil {
			return err
		}
	}

	return nil
}

func (exec *PipelineExecutor) executeInputProcessing(ctx context.Context, node *amqpRpcPair) error {
	if err := exec.pumpBatchIntoInput(ctx, node); err != nil {
		return err
	}

	events, err := exec.readRespEvents(ctx, node.resp)

	if err != nil {
		return err
	}

	isDone := false

	for event := range events {
		switch event.Status {
		case rpc.OK:
			if err := exec.processOkEvent(ctx, event, node); err != nil {
				return err
			}
		case rpc.ERROR:
			if err := exec.processErrorEvent(ctx, event, node); err != nil {
				return err
			}
		case rpc.ALLDONE:
			isDone = true
			if err := exec.processAllDoneEvent(ctx, event, node); err != nil {
				return err
			}
		}

		if !isDone {
			if err := exec.pumpBatchIntoInput(ctx, node); err != nil {
				return err
			}
		}

		err = event.Ack(false)
		if err != nil {
			return err
		}
	}

	return nil
}
