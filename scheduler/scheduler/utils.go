package scheduler

import (
	"sort"

	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler/evaluator"
	"github.com/kevwan/mapreduce/v2"
)

func sortNodes(candidates []*resource.Peer, eval evaluator.Evaluator, peer *resource.Peer, total int32) {
	switch eval.EvalType() {
	case evaluator.MLAlgorithm:
		compute(candidates, peer, eval)
	default:
		sort.Slice(
			candidates,
			func(i, j int) bool {
				return eval.Evaluate(candidates[i], peer, total) > eval.Evaluate(candidates[j], peer, total)
			},
		)
	}
}

// TODO
func compute(candidates []*resource.Peer, peer *resource.Peer, eval evaluator.Evaluator) {
	mapreduce.MapReduce(func(source chan<- *resource.Peer) {
		// generator
		for _, parent := range candidates {
			source <- parent
		}
	}, func(parent *resource.Peer, writer mapreduce.Writer[float64], cancel func(error)) {
		// mapper
		writer.Write(eval.Evaluate(parent, peer, 0))
	}, func(pipe <-chan float64, writer mapreduce.Writer[int], cancel func(error)) {
		// reducer
		var sum int
		for i := range pipe {
			i++
		}
		writer.Write(sum)
	})
}
