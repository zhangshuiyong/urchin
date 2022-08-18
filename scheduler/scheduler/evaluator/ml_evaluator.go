package evaluator

import (
	"d7y.io/dragonfly/v2/scheduler/resource"
)

type MLEvaluator struct {
	w *Watcher
}

// TODO mapreduce
func (mle *MLEvaluator) Evaluate(parent *resource.Peer, child *resource.Peer, taskPieceCount int32) float64 {
	return 0
}

func (mle *MLEvaluator) IsBadNode(peer *resource.Peer) bool {
	return NormalIsBadNode(peer)
}

func NewMLEvaluator() Evaluator {
	mle := &MLEvaluator{
		w: NewWatcher(),
	}
	mle.w.DetectVersion()
	return mle
}

func (mle *MLEvaluator) LoadModel() {

}
