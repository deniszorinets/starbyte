package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"starbyte.io/coordinator/db"
	"starbyte.io/core/amqp"
	"starbyte.io/core/utils"
)

type amqpRpcPair struct {
	vertex db.PipelineTopologyVertex
	req    string
	resp   string
	next   []*amqpRpcPair
}

type PipelineExecutor struct {
	pipeline        *db.Pipeline
	rpcChannels     []*amqpRpcPair
	inputRpcChannel *amqpRpcPair
	amqpConn        amqp.Amqp
	s3ConnStr       string
	dbRepository    db.Repository
}

func findAllOutVertex(vertex db.PipelineTopologyVertex, edges []db.PipelineTopologyEdge) []db.PipelineTopologyVertex {
	result := []db.PipelineTopologyVertex{}
	for _, edge := range edges {
		if edge.From.Name == vertex.Name {
			result = append(result, edge.To)
		}
	}
	return result
}

func findRpcNodePairByVertex(vertex db.PipelineTopologyVertex, pairs []*amqpRpcPair) *amqpRpcPair {
	for _, pair := range pairs {
		if pair.vertex.Name == vertex.Name {
			return pair
		}
	}
	return nil
}

func initRpcChannels(topology *db.PipelineGraph) []*amqpRpcPair {
	rpcChannels := []*amqpRpcPair{}

	for _, node := range topology.Nodes {
		requestAmqpQueueName := node.Step.GetReqQueueName()
		responseAmqpQueueName := node.Step.GetRespQueueName()

		rpcChannels = append(rpcChannels, &amqpRpcPair{vertex: node, req: requestAmqpQueueName, resp: responseAmqpQueueName, next: []*amqpRpcPair{}})
	}

	for _, amqpPair := range rpcChannels {
		outboundVertex := findAllOutVertex(amqpPair.vertex, topology.Edges)
		for _, vertex := range outboundVertex {
			nextPair := findRpcNodePairByVertex(vertex, rpcChannels)
			amqpPair.next = append(amqpPair.next, nextPair)
		}
	}

	for _, amqpPair := range rpcChannels {
		if len(amqpPair.next) == 0 {
			amqpPair.next = nil
		}
	}

	return rpcChannels

}

func NewExecutor(pipeline *db.Pipeline, amqpConn amqp.Amqp, s3ConnStr string, dbRepository db.Repository) (*PipelineExecutor, error) {
	topology, err := pipeline.GetTopology()
	if err != nil {
		return nil, err
	}

	rpcChannels := initRpcChannels(topology)
	var inputRpcChannel *amqpRpcPair

	for _, rpc := range rpcChannels {
		if rpc.vertex.Step.Input == "" {
			inputRpcChannel = rpc
			break
		}
	}

	return &PipelineExecutor{
		pipeline:        pipeline,
		rpcChannels:     rpcChannels,
		amqpConn:        amqpConn,
		inputRpcChannel: inputRpcChannel,
		s3ConnStr:       s3ConnStr,
		dbRepository:    dbRepository,
	}, nil

}

func (exec *PipelineExecutor) ensureQueueDeclared() error {
	for _, pair := range exec.rpcChannels {
		reqErr := exec.amqpConn.QueueDeclare(pair.req)
		respErr := exec.amqpConn.QueueDeclare(pair.resp)

		if reqErr != nil || respErr != nil {
			return errors.Join(reqErr, respErr)
		}
	}
	return nil
}

func (exec *PipelineExecutor) getNewResultUri() string {
	uid := uuid.New()
	return fmt.Sprintf("%s/%s.cbor.tar.gz", exec.s3ConnStr, uid.String())
}

func (exec *PipelineExecutor) newCorrelationId() uuid.UUID {
	return uuid.New()
}

func (exec *PipelineExecutor) Run() error {
	if err := exec.ensureQueueDeclared(); err != nil {
		return err
	}

	wg := &sync.WaitGroup{}

	workersContext := context.Background()

	utils.RunInWg(wg, func() { exec.executeInputProcessing(workersContext, exec.inputRpcChannel) })

	for _, pair := range exec.rpcChannels {
		if pair == exec.inputRpcChannel {
			continue
		}

		wg.Add(1)
		go func(p *amqpRpcPair, c context.Context) {
			defer wg.Done()
			exec.executeNodeProcessing(c, p)
		}(pair, workersContext)
	}

	wg.Wait()

	return nil
}
