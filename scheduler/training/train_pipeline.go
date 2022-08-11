package training

import (
	"context"
	"fmt"
	"io"

	"d7y.io/dragonfly/v2/scheduler/storage"

	"d7y.io/dragonfly/v2/pkg/pipeline"
)

type Training struct {
	dataInstance *Data
	*pipeline.StepInfra
}

// GetSource actually function.
func (train *Training) GetSource(req *pipeline.Request) (*pipeline.Request, error) {
	result, err := train.dataInstance.PreProcess()
	if err != nil {
		return nil, err
	}

	return &pipeline.Request{
		Data:   result,
		KeyVal: req.KeyVal,
	}, nil
}

func (train *Training) getReader(req *pipeline.Request) (io.ReadCloser, error) {
	store := req.Data.(storage.Storage)
	reader, err := store.Open()
	if err != nil {
		return nil, err
	}
	return reader, nil
}

// Serve interface.
func (train *Training) Serve(req *pipeline.Request, out chan *pipeline.Request) error {
	reader, err := train.getReader(req)
	if err != nil {
		return err
	}

	defer func() {
		// TODO error handle
		train.dataInstance.Reader.Close()
	}()

	dataInstance, err := New(reader)
	if err != nil {
		return err
	}
	train.dataInstance = dataInstance

	for {
		source, err := train.GetSource(req)
		if err != nil {
			return err
		}
		if source == nil {
			close(out)
			break
		}

		out <- source
	}
	return nil
}

func (train *Training) TrainCall(ctx context.Context, in chan *pipeline.Request, out chan *pipeline.Request) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("training process has been canceled")
		case val := <-in:
			if val == nil {
				return nil
			}
			err := train.Serve(val, out)
			if err != nil {
				return err
			}
		}
	}
}

func NewTrainStep() pipeline.Step {
	t := Training{}
	t.StepInfra = pipeline.New("Training", t.TrainCall)
	return t
}
