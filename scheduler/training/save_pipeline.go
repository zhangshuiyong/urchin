package training

import (
	"context"
	"fmt"

	"d7y.io/dragonfly/v2/manager/types"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/scheduler/config"

	"d7y.io/dragonfly/v2/pkg/pipeline"
)

type Saving struct {
	*pipeline.StepInfra
}

// GetSource actually function.
func (save *Saving) GetSource(req *pipeline.Request) (*string, error) {
	request := req.Data.(*types.CreateModelVersionRequest)
	if request == nil {
		return nil, fmt.Errorf("request get nil data")
	}

	dynconfig := req.KeyVal[DynConfigData].(*config.DynconfigData)
	if dynconfig == nil {
		return nil, fmt.Errorf("lose keyVal dynconfig")
	}

	mc := req.KeyVal[ManagerClient].(client.Client)
	if mc == nil {
		return nil, fmt.Errorf("lose keyVal ManagerClient")
	}

	mn := req.KeyVal[ModelName].(string)
	mi := GenerateModelID(mn)
	_, err := mc.CreateModel(context.Background(), &managerv1.CreateModelRequest{
		ModelId:     mi,
		Name:        mn,
		SchedulerId: dynconfig.SchedulerCluster.ID,
		HostName:    dynconfig.Hostname,
		Ip:          dynconfig.IP,
	})
	if err != nil {
		return nil, err
	}

	modelVersionRequest, err := save.handleReq(request, dynconfig.SchedulerCluster.ID, mi)
	if err != nil {
		return nil, err
	}

	version, err := mc.CreateModelVersion(context.Background(), modelVersionRequest)
	if err != nil {
		return nil, err
	}

	_, err = mc.UpdateModel(context.Background(), &managerv1.UpdateModelRequest{
		ModelId:   mi,
		VersionId: version.VersionId,
	})
	if err != nil {
		return nil, err
	}
	return &version.VersionId, nil
}

func (save *Saving) handleReq(src *types.CreateModelVersionRequest, ID uint64, mi string) (*managerv1.CreateModelVersionRequest, error) {
	return &managerv1.CreateModelVersionRequest{
		SchedulerId: ID,
		ModelId:     mi,
		Data:        src.Data,
		Mae:         src.MAE,
		Mse:         src.MSE,
		Rmse:        src.RMSE,
		R2:          src.R2,
	}, nil
}

// Serve interface.
func (save *Saving) Serve(req *pipeline.Request) (*string, error) {
	return save.GetSource(req)
}

func (save *Saving) SaveCall(ctx context.Context, in chan *pipeline.Request, out chan *pipeline.Request) error {
	var (
		mv  *string
		err error
	)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("saving process has been canceled")
		case val := <-in:
			if val == nil {
				out <- &pipeline.Request{
					Data:   mv,
					KeyVal: nil,
				}
				return nil
			}
			mv, err = save.Serve(val)
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
