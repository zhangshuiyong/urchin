package training

import (
	"context"
	"fmt"

	"d7y.io/dragonfly/v2/scheduler/training/models"

	"d7y.io/dragonfly/v2/pkg/pipeline"
)

type Evaluating struct {
	eval *Eval
	*pipeline.StepInfra
}

func (eva *Evaluating) GetSource(req *pipeline.Request) error {
	model := req.KeyVal[OutPutModel].(*models.LinearRegression)
	if model == nil {
		return fmt.Errorf("lose model")
	}
	// TODO
	//model.Predict()
	return nil
}

func (eva *Evaluating) Serve(req *pipeline.Request, out chan *pipeline.Request) error {
	err := eva.GetSource(req)
	if err != nil {
		return err
	}
	return nil
}

func (eva *Evaluating) evaCall(ctx context.Context, in chan *pipeline.Request, out chan *pipeline.Request) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("evaluating process has been canceled")
		case val := <-in:
			if val == nil {
				out <- &pipeline.Request{

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

func NewEvaStep() pipeline.Step {
	eva := Evaluating{}
	eva.StepInfra = pipeline.New("evaluating", eva.evaCall)
	return eva
}
