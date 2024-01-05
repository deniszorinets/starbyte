package db

import (
	"errors"
	"testing"

	"starbyte.io/core/utils"
)

func TestBuildGraph(t *testing.T) {
	pipeline := Pipeline{
		Name: "test_pipeline",
		Steps: map[string]Step{
			"input": {Input: ""},
			"step1": {Input: "input"},
			"step2": {Input: "step1"},
			"step3": {Input: "step2"},
			"step4": {Input: "input"},
			"step5": {Input: "step4"},
			"step6": {Input: "step7"},
		},
	}

	topology := pipeline.buildGraph()
	expectedTopology := map[string][]string{
		"":      {"input"},
		"input": {"step1", "step4"},
		"step1": {"step2"},
		"step2": {"step3"},
		"step4": {"step5"},
		"step7": {"step6"},
	}

	for k, v := range topology {
		expectedValues, ok := expectedTopology[k]
		if !ok {
			t.Fatal("exptected topology is different")
		}

		if len(utils.Intersect(v, expectedValues)) != len(expectedValues) {
			t.Fatal("exptected topology is different")
		}
	}
}

func TestTraverseNoInputError(t *testing.T) {
	pipeline := Pipeline{
		Name: "test_pipeline",
		Steps: map[string]Step{
			"input": {Input: "step0"},
			"step1": {Input: "input"},
			"step2": {Input: "step1"},
			"step3": {Input: "step2"},
			"step4": {Input: "input"},
			"step5": {Input: "step4"},
			"step6": {Input: "step7"},
		},
	}

	topology := pipeline.buildGraph()
	_, err := traverse("", topology)

	if !errors.Is(err, ErrNoInput) {
		t.Fatalf("no input error is expected")
	}
}

func TestTraverse(t *testing.T) {
	pipeline := Pipeline{
		Name: "test_pipeline",
		Steps: map[string]Step{
			"input": {Input: ""},
			"step1": {Input: "input"},
			"step2": {Input: "step1"},
			"step3": {Input: "step2"},
			"step4": {Input: "step3"},
			"step5": {Input: "step4"},
			"step6": {Input: "step7"},
		},
	}

	topology := pipeline.buildGraph()
	result, err := traverse("", topology)

	if err != nil {
		t.Fatalf("error %e", err)
	}

	expectedResult := map[string]byte{
		"":      1,
		"input": 1,
		"step1": 1,
		"step2": 1,
		"step3": 1,
		"step4": 1,
		"step5": 1,
	}

	if len(utils.Intersect(utils.Keys(result), utils.Keys(expectedResult))) != len(expectedResult) {
		t.Fatal("exptected traverse is different")
	}
}

func TestCleanup(t *testing.T) {
	pipeline := Pipeline{
		Name: "test_pipeline",
		Steps: map[string]Step{
			"input": {Input: ""},
			"step1": {Input: "input"},
			"step2": {Input: "step1"},
			"step3": {Input: "step2"},
			"step4": {Input: "input"},
			"step5": {Input: "step4"},
			"step6": {Input: "step7"},
		},
	}

	topology := pipeline.buildGraph()
	traverse, err := traverse("", topology)
	if err != nil {
		t.Fatalf("error %e", err)
	}
	cleanup(topology, traverse)
	expectedTopology := map[string][]string{
		"":      {"input"},
		"input": {"step1", "step4"},
		"step1": {"step2"},
		"step2": {"step3"},
		"step4": {"step5"},
	}

	for k, v := range topology {
		expectedValues, ok := expectedTopology[k]
		if !ok {
			t.Fatal("exptected topology is different")
		}

		if len(utils.Intersect(v, expectedValues)) != len(expectedValues) {
			t.Fatal("exptected topology is different")
		}
	}
}

func TestGetTopology(t *testing.T) {
	pipeline := Pipeline{
		Name: "test_pipeline",
		Steps: map[string]Step{
			"input": {Input: ""},
			"step1": {Input: "input"},
			"step2": {Input: "step1"},
			"step3": {Input: "step2"},
			"step4": {Input: "input"},
			"step5": {Input: "step4"},
			"step6": {Input: "step7"},
		},
	}

	result, err := pipeline.GetTopology()
	if err != nil {
		t.Fatalf("error %e", err)
	}

	expected := PipelineGraph{
		Nodes: []PipelineTopologyVertex{
			{Step{Input: ""}, "input"},
			{Step{Input: "input"}, "step1"},
			{Step{Input: "step1"}, "step2"},
			{Step{Input: "step2"}, "step3"},
			{Step{Input: "input"}, "step4"},
			{Step{Input: "step4"}, "step5"},
		},
		Edges: []PipelineTopologyEdge{
			{From: PipelineTopologyVertex{Step{Input: ""}, "input"}, To: PipelineTopologyVertex{Step{Input: "input"}, "step1"}},
			{From: PipelineTopologyVertex{Step{Input: "input"}, "step1"}, To: PipelineTopologyVertex{Step{Input: "step1"}, "step2"}},
			{From: PipelineTopologyVertex{Step{Input: "step1"}, "step2"}, To: PipelineTopologyVertex{Step{Input: "step2"}, "step3"}},
			{From: PipelineTopologyVertex{Step{Input: ""}, "input"}, To: PipelineTopologyVertex{Step{Input: "input"}, "step4"}},
			{From: PipelineTopologyVertex{Step{Input: "input"}, "step4"}, To: PipelineTopologyVertex{Step{Input: "step4"}, "step5"}},
		},
	}

	if len(result.Nodes) != len(expected.Nodes) || len(utils.Intersect(result.Nodes, expected.Nodes)) != len(result.Nodes) {
		t.Fatalf("result pipeline graph nodes different from expected")
	}

	if len(result.Edges) != len(expected.Edges) || len(utils.Intersect(result.Edges, expected.Edges)) != len(result.Edges) {
		t.Fatalf("result pipeline graph edges different from expected")
	}
}

func TestGetTopologNoInputError(t *testing.T) {
	pipeline := Pipeline{
		Name: "test_pipeline",
		Steps: map[string]Step{
			"input": {Input: "step0"},
			"step1": {Input: "input"},
			"step2": {Input: "step1"},
			"step3": {Input: "step2"},
			"step4": {Input: "input"},
			"step5": {Input: "step4"},
			"step6": {Input: "step7"},
		},
	}

	_, err := pipeline.GetTopology()
	if !errors.Is(err, ErrNoInput) {
		t.Fatalf("no input error is expected")
	}
}
