package training

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"d7y.io/dragonfly/v2/pkg/pipeline"
)

type Loading struct {
	*pipeline.StepInfra
}

// GetSource actually function.
func (load *Loading) GetSource(req *pipeline.Request) (*pipeline.Request, error) {
	source := req.Data.([]byte)

	var result map[float64]*LinearModel
	dec := gob.NewDecoder(bytes.NewBuffer(source))
	err := dec.Decode(&result)
	if err != nil {
		return nil, err
	}

	return &pipeline.Request{
		Data:   result,
		KeyVal: req.KeyVal,
	}, nil
}

// Serve interface.
func (load *Loading) Serve(req *pipeline.Request) (*pipeline.Request, error) {
	return load.GetSource(req)
}

// TODO
func (l *Loading) LoadCall(ctx context.Context, in chan *pipeline.Request) (*pipeline.Request, error) {
	for {
		// TODO out change to the answer struct
		out := &pipeline.Request{}
		var err error

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("loading process has been canceled")
		case val := <-in:
			if val == nil {
				return out, nil
			}
			out, err = l.Serve(val)
			if err != nil {
				return nil, err
			}
		}
	}
}

func NewLoadingStep() pipeline.Step {
	l := Loading{}
	l.StepInfra = pipeline.New("Loading", l.LoadCall)
	return l
}
