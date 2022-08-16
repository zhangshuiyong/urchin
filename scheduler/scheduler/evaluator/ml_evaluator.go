package evaluator

import (
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

type MLEvaluator struct {
	modelVersion types.ModelVersion
	model        types.Model
}

// TODO mapreduce
func (mle *MLEvaluator) Evaluate(parent *resource.Peer, child *resource.Peer, taskPieceCount int32) float64 {
	return 0
}

func (mle *MLEvaluator) IsBadNode(peer *resource.Peer) bool {
	return NormalIsBadNode(peer)
}

func NewMLEvaluator() Evaluator {
	return &MLEvaluator{}
}

func (mle *MLEvaluator) LoadModel() {
	// TODO if no model exist, use evaluate_base
	// 第一次如何找到对应model id
}
