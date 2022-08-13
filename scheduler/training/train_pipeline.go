package training

import (
	"context"
	"fmt"

	"d7y.io/dragonfly/v2/scheduler/training/models"

	"d7y.io/dragonfly/v2/pkg/pipeline"

	"github.com/sjwhitworth/golearn/base"
)

type Training struct {
	// model preserve
	model *models.LinearRegression
	to    *TrainOptions
	*pipeline.StepInfra
}

func (t *Training) GetSource(req *pipeline.Request) error {
	source := req.Data.(*base.DenseInstances)
	err := TrainProcess(source, t.to, t.model)
	if err != nil {
		return err
	}

	return nil
}

func (t *Training) Serve(req *pipeline.Request, out chan *pipeline.Request) error {
	t.to = NewTrainOptions()
	err := t.GetSource(req)
	if err != nil {
		return err
	}
	return nil
}

func (t *Training) trainCall(ctx context.Context, in chan *pipeline.Request, out chan *pipeline.Request) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("training process has been canceled")
		case val := <-in:
			if val == nil {
				out <- &pipeline.Request{
					Data: t.model,
					// TODO
					KeyVal: nil,
				}
				return nil
			}
			err := t.Serve(val, out)
			if err != nil {
				return err
			}
		}
	}
}

func NewTrainStep() pipeline.Step {
	e := Training{}
	e.StepInfra = pipeline.New("training", e.trainCall)
	return e
}
