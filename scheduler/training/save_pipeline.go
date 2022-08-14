package training

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"d7y.io/dragonfly/v2/manager/types"

	"d7y.io/dragonfly/v2/pkg/pipeline"
)

type Saving struct {
	*pipeline.StepInfra
}

// GetSource actually function.
func (save *Saving) GetSource(req *pipeline.Request) error {
	source := req.Data.(*types.CreateModelVersionRequest)
	if source == nil {
		return fmt.Errorf("lose create params")
	}
	err := save.managerSave(source)
	if err != nil {
		return err
	}
	return nil
}

func (save *Saving) managerSave(param *types.CreateModelVersionRequest) error {
	// TODO model_id should get from keyVal, if not get creat it
	id := "1"
	modelID := "2"
	url := fmt.Sprintf("/api/v1/schedulers/%s/models/%s/versions", id, modelID)

	body, err := json.Marshal(param)
	if err != nil {
		return err
	}
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	cli := &http.Client{
		Timeout: 5 * time.Second,
	}

	response, err := cli.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode == http.StatusOK {
		return nil
	} else {
		return fmt.Errorf("response is err, error code is %d", response.StatusCode)
	}
}

// Serve interface.
func (save *Saving) Serve(req *pipeline.Request) error {
	return save.GetSource(req)
}

func (save *Saving) SaveCall(ctx context.Context, in chan *pipeline.Request, out chan *pipeline.Request) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("saving process has been canceled")
		case val := <-in:
			if val == nil {
				out <- &pipeline.Request{
					Data: "success done",
					// TODO
					KeyVal: nil,
				}
				return nil
			}
			err := save.Serve(val)
			if err != nil {
				return err
			}
		}
	}
}

func NewSavingStep() pipeline.Step {
	s := Saving{}
	s.StepInfra = pipeline.New("Saving", s.SaveCall)
	return s
}
