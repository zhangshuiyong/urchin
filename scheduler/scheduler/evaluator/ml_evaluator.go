package evaluator

import (
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

type MLEvaluator struct {
	cfg          config.DynconfigInterface
	needVersion  chan uint64
	modelVersion chan *types.ModelVersion
	model        *types.ModelVersion
}

// TODO mapreduce
func (mle *MLEvaluator) Evaluate(parent *resource.Peer, child *resource.Peer, taskPieceCount int32) float64 {
	return 0
}

func (mle *MLEvaluator) IsBadNode(peer *resource.Peer) bool {
	return NormalIsBadNode(peer)
}

func (mle *MLEvaluator) LoadModel() {
	configData, err := mle.cfg.Get()
	if err != nil {
		return
	}
	mle.needVersion <- configData.SchedulerCluster.ID
	model, ok := <-mle.modelVersion
	if ok {
		mle.model = model
	}
}

func (mle *MLEvaluator) EvalType() string {
	return MLAlgorithm
}

func NewMLEvaluator(dynconfig config.DynconfigInterface, needVersion chan uint64, modelVersion chan *types.ModelVersion) Evaluator {
	return &MLEvaluator{
		cfg:          dynconfig,
		needVersion:  needVersion,
		modelVersion: modelVersion,
	}
}
