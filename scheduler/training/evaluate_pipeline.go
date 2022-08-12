package training

import (
	"context"
	"fmt"

	"d7y.io/dragonfly/v2/scheduler/training/models"

	"d7y.io/dragonfly/v2/pkg/pipeline"

	"github.com/sjwhitworth/golearn/base"
)

type Evaluating struct {
	// model preserve
	model map[float64]*models.LinearRegression
	to    *TrainOptions
	*pipeline.StepInfra
}

func (eva *Evaluating) GetSource(req *pipeline.Request) error {
	source := req.Data.(*base.DenseInstances)
	_, err := TrainProcess(source, eva.to)
	if err != nil {
		return err
	}

	return nil
}

func (eva *Evaluating) Serve(req *pipeline.Request, out chan *pipeline.Request) error {
	eva.to = NewTrainOptions()
	err := eva.GetSource(req)
	if err != nil {
		return err
	}
	return nil
}

func (eva *Evaluating) EvalCall(ctx context.Context, in chan *pipeline.Request, out chan *pipeline.Request) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("evaluating process has been canceled")
		case val := <-in:
			if val == nil {
				out <- &pipeline.Request{
					Data: eva.model,
					// TODO
					KeyVal: nil,
				}
				return nil
			}
			err := eva.Serve(val, out)
			if err != nil {
				return err
			}
		}
	}
}

func NewEvalStep() pipeline.Step {
	e := Evaluating{}
	e.StepInfra = pipeline.New("Evaluating", e.EvalCall)
	return e
}
