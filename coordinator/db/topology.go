package db

import (
	"errors"

	"starbyte.io/core/utils"
)

var (
	ErrNoInput = errors.New("topology does not have input")
)

type PipelineTopologyVertex struct {
	Step Step
	Name string
}

type PipelineTopologyEdge struct {
	From PipelineTopologyVertex
	To   PipelineTopologyVertex
}

type PipelineGraph struct {
	Nodes []PipelineTopologyVertex
	Edges []PipelineTopologyEdge
}

func traverse(startNode string, topology map[string][]string) (map[string]byte, error) {
	if startNode == "" && !utils.HasKey(topology, startNode) {
		return nil, ErrNoInput
	}

	visited := map[string]byte{startNode: 1}

	for _, node := range topology[startNode] {
		visitedNodes, _ := traverse(node, topology)
		utils.MergeMap(visited, visitedNodes)
	}

	return visited, nil
}

func cleanup(topology map[string][]string, visitedNodes map[string]byte) {
	for k := range topology {
		if !utils.HasKey(visitedNodes, k) {
			delete(topology, k)
		}
	}
}

func (pipeline *Pipeline) buildGraph() map[string][]string {
	outputs := map[string][]string{}

	for stepName, step := range pipeline.Steps {
		if utils.HasKey(outputs, step.Input) {
			outputs[step.Input] = append(outputs[step.Input], stepName)
		} else {
			outputs[step.Input] = []string{stepName}
		}

	}
	return outputs
}

func (pipeline *Pipeline) GetTopology() (*PipelineGraph, error) {
	topology := pipeline.buildGraph()
	traverse, err := traverse("", topology)
	if err != nil {
		return nil, err
	}
	cleanup(topology, traverse)

	edges := []PipelineTopologyEdge{}
	nodes := []PipelineTopologyVertex{}

	for node, outputs := range topology {
		from := PipelineTopologyVertex{pipeline.Steps[node], node}
		for _, output := range outputs {
			to := PipelineTopologyVertex{pipeline.Steps[output], output}
			nodes = append(nodes, to)
			if node == "" {
				continue
			}
			edges = append(edges, PipelineTopologyEdge{From: from, To: to})
		}
	}

	return &PipelineGraph{Nodes: nodes, Edges: edges}, nil
}
