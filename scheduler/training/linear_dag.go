package training

import (
	"d7y.io/dragonfly/v2/pkg/dag"
	"d7y.io/dragonfly/v2/pkg/pipeline"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

type LinearTraining struct {
	graph   dag.DAG[pipeline.StepConstruct]
	storage storage.Storage
}

func NewLinearTraining(storage storage.Storage) *LinearTraining {
	g, err := LinearDag()
	if err != nil {
		return nil
	}
	return &LinearTraining{
		graph:   g,
		storage: storage,
	}
}

var (
	mapStep map[string]pipeline.StepConstruct
	order   []string
)

func init() {
	mapStep = map[string]pipeline.StepConstruct{
		"LoadingData": NewLoadStep,
		"Training":    NewTrainStep,
		"LoadingTest": NewLoadStep,
		"Evaluating":  NewEvaStep,
		"Saving":      NewSavingStep,
	}
	order = []string{"LoadingData", "Training", "LoadingTest", "Evaluating", "Saving"}
}

func LinearDag() (dag.DAG[pipeline.StepConstruct], error) {
	graph := dag.NewDAG[pipeline.StepConstruct]()

	for k, v := range mapStep {
		err := graph.AddVertex(k, v)
		if err != nil {
			return nil, err
		}
	}

	for i := 0; i < len(order)-1; i++ {
		err := graph.AddEdge(order[i], order[i+1])
		if err != nil {
			return nil, err
		}
	}
	return graph, nil
}

func (lr *LinearTraining) Process() interface{} {
	p := pipeline.NewPipeline()
	req := &pipeline.Request{
		KeyVal: make(map[string]interface{}),
		Data:   lr.storage,
	}

	req, err := p.Exec(req, lr.graph)
	if err != nil {
		return nil
	}
	return req.Data
}
