package pipeline

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"starbyte.io/coordinator/db"
	s3io "starbyte.io/core/s3"
	"starbyte.io/core/utils"
)

var (
	inputUUID = uuid.New()
	step1UUID = uuid.New()
	step2UUID = uuid.New()
	step3UUID = uuid.New()
	step4UUID = uuid.New()
	step5UUID = uuid.New()
)

func getNextVartexNames(pair *amqpRpcPair) []string {
	if pair.next == nil {
		return []string{}
	}
	result := make([]string, len(pair.next))
	for idx, pair := range pair.next {
		result[idx] = pair.vertex.Name
	}
	return result
}

func checkQueueNames(t *testing.T, c *amqpRpcPair, nodeUid uuid.UUID, nodeName string) {
	expected_req := fmt.Sprintf("requests_%s_%s", nodeUid, nodeName)
	if c.req != expected_req {
		t.Errorf("wrong request queue name %s instead of %s", c.req, expected_req)
	}

	expected_rsp := fmt.Sprintf("responses_%s_%s", nodeUid, nodeName)
	if c.req != expected_req {
		t.Errorf("wrong response queue name %s instead of %s", c.req, expected_rsp)
	}
}

func TestInitRpcChannels(t *testing.T) {
	topology, err := pipeline.GetTopology()
	if err != nil {
		t.Errorf("failed with topology build error: %s", err)
	}

	rpcChannels := initRpcChannels(topology)
	for _, c := range rpcChannels {
		switch c.vertex.Name {
		case "input":
			if len(utils.Intersect(getNextVartexNames(c), []string{"step1", "step4"})) != 2 {
				t.Error("wrong input step next array")
			}
			checkQueueNames(t, c, inputUUID, "input")

		case "step1":
			if len(utils.Intersect(getNextVartexNames(c), []string{"step2"})) != 1 {
				t.Error("wrong input step next array")
			}
			checkQueueNames(t, c, step1UUID, "step1")
		case "step2":
			if len(utils.Intersect(getNextVartexNames(c), []string{"step3"})) != 1 {
				t.Error("wrong input step next array")
			}
			checkQueueNames(t, c, step2UUID, "step2")
		case "step3":
			if c.next != nil {
				t.Error("wrong input step next array")
			}
			checkQueueNames(t, c, step3UUID, "step3")
		case "step4":
			if len(utils.Intersect(getNextVartexNames(c), []string{"step5"})) != 1 {
				t.Error("wrong input step next array")
			}
			checkQueueNames(t, c, step4UUID, "step4")
		case "step5":
			if c.next != nil {
				t.Error("wrong input step next array")
			}
			checkQueueNames(t, c, step5UUID, "step5")
		}

	}
}

func TestFindAllOutVertexSuccess(t *testing.T) {
	topology, err := pipeline.GetTopology()
	if err != nil {
		t.Errorf("failed with topology build error: %s", err)
	}

	outVertex := findAllOutVertex(db.PipelineTopologyVertex{Name: "input"}, topology.Edges)

	outVertexNames := []string{}
	for _, v := range outVertex {
		outVertexNames = append(outVertexNames, v.Name)
	}
	if len(utils.Intersect(outVertexNames, []string{"step1", "step4"})) != 2 {
		t.Error("wrong out vertex array")
	}
}

func TestFindAllOutVertexFail(t *testing.T) {
	pipeline := db.Pipeline{
		Name: "test_pipeline",
		Steps: map[string]db.Step{
			"input": {Input: "", Id: inputUUID},
			"step1": {Input: "input", Id: step1UUID},
			"step2": {Input: "step1", Id: step2UUID},
			"step3": {Input: "step2", Id: step3UUID},
			"step4": {Input: "input", Id: step4UUID},
			"step5": {Input: "step4", Id: step5UUID},
		},
	}

	topology, err := pipeline.GetTopology()
	if err != nil {
		t.Errorf("failed with topology build error: %s", err)
	}

	outVertex := findAllOutVertex(db.PipelineTopologyVertex{Name: "not_exists"}, topology.Edges)

	if len(outVertex) != 0 {
		t.Error("wrong out vertex array")
	}
}

func TestFindRpcNodePairByVertexSuccess(t *testing.T) {
	topology, err := pipeline.GetTopology()
	if err != nil {
		t.Errorf("failed with topology build error: %s", err)
	}

	rpcChannels := initRpcChannels(topology)
	result := findRpcNodePairByVertex(db.PipelineTopologyVertex{Name: "input"}, rpcChannels)
	if result == nil || result.vertex.Name != "input" {
		t.Error("wrong rpc pair found")
	}
}

func TestFindRpcNodePairByVertexFail(t *testing.T) {
	pipeline := db.Pipeline{
		Name: "test_pipeline",
		Steps: map[string]db.Step{
			"input": {Input: "", Id: inputUUID},
			"step1": {Input: "input", Id: step1UUID},
			"step2": {Input: "step1", Id: step2UUID},
			"step3": {Input: "step2", Id: step3UUID},
			"step4": {Input: "input", Id: step4UUID},
			"step5": {Input: "step4", Id: step5UUID},
		},
	}

	topology, err := pipeline.GetTopology()
	if err != nil {
		t.Errorf("failed with topology build error: %s", err)
	}

	rpcChannels := initRpcChannels(topology)
	result := findRpcNodePairByVertex(db.PipelineTopologyVertex{Name: "not_exists"}, rpcChannels)
	if result != nil {
		t.Error("wrong rpc pair found")
	}
}

func TestNewExecutorSuccess(t *testing.T) {
	executor, err := NewExecutor(&pipeline, nil, "", nil)

	if err != nil {
		t.Errorf("executor creation error")
	}

	expectedRpcPairInput := findRpcNodePairByVertex(db.PipelineTopologyVertex{Name: "input"}, executor.rpcChannels)

	if executor.inputRpcChannel != expectedRpcPairInput {
		t.Errorf("wrong input")
	}
}

func TestNewExecutorFail(t *testing.T) {
	pipeline := db.Pipeline{
		Name: "test_pipeline",
		Steps: map[string]db.Step{
			"input": {Input: "wrong_node", Id: inputUUID, Name: "input"},
			"step1": {Input: "input", Id: step1UUID, Name: "step1"},
			"step2": {Input: "step1", Id: step2UUID, Name: "step2"},
			"step3": {Input: "step2", Id: step3UUID, Name: "step3"},
			"step4": {Input: "input", Id: step4UUID, Name: "step4"},
			"step5": {Input: "step4", Id: step5UUID, Name: "step5"},
		},
	}

	_, err := NewExecutor(&pipeline, nil, "", nil)

	if err == nil {
		t.Errorf("error is expected")
	}
}

func TestEnsureQueueDeclared(t *testing.T) {
	amqpMock := NewAmqpMock()

	executor, err := NewExecutor(&pipeline, amqpMock, "", nil)
	if err != nil {
		t.Errorf("executor creation error")
	}

	executor.ensureQueueDeclared()

	if len(amqpMock.Queues) != 12 {
		t.Errorf("queue creation error")
	}

}

func TestEnsureQueueDeclaredFail(t *testing.T) {
	amqpMock := NewAmqpErrorMock()

	executor, err := NewExecutor(&pipeline, amqpMock, "", nil)
	if err != nil {
		t.Errorf("executor creation error")
	}

	err = executor.ensureQueueDeclared()

	if err == nil {
		t.Errorf("error is expected")
	}
}

func TestGetNewResultUri(t *testing.T) {
	executor, err := NewExecutor(&pipeline, nil, "https://test:test@test.com/test", nil)
	if err != nil {
		t.Errorf("executor creation error")
	}

	result := executor.getNewResultUri()

	if !s3io.UriRegexp.Match([]byte(result)) {
		t.Errorf("wrong file uri")
	}
}

func TestNewCorrelationId(t *testing.T) {
	executor, err := NewExecutor(&pipeline, nil, "https://test:test@test.com/test", nil)
	if err != nil {
		t.Errorf("executor creation error")
	}

	result := executor.newCorrelationId()

	if _, err := uuid.Parse(result.String()); err != nil {
		t.Errorf("wrong uid")
	}
}

func TestExecuteInputProcessing(t *testing.T) {
	executor, err := NewExecutor(&pipeline, nil, "https://test:test@test.com/test", nil)
	if err != nil {
		t.Errorf("executor creation error")
	}

	result := executor.newCorrelationId()

	if _, err := uuid.Parse(result.String()); err != nil {
		t.Errorf("wrong uid")
	}
}
