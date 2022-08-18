package evaluator

import (
	"context"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client"
)

type Watcher struct {
	version      chan string
	modelVersion chan *types.ModelVersion
	done         chan struct{}
	mc           client.Client
}

// TODO
func (w *Watcher) DetectVersion() {
	go func() {
		for {
			select {
			case v := <-w.version:
				version, err := w.mc.GetModelVersion(context.Background(), &managerv1.GetModelVersionRequest{
					VersionId:   v,
					SchedulerId: 1,
					ModelId:     "1",
				})
				if err != nil {
					return
				}
				w.modelVersion <- &types.ModelVersion{
					ID:   v,
					MAE:  version.Mae,
					MSE:  version.Mse,
					RMSE: version.Rmse,
					R2:   version.R2,
					Data: version.Data,
				}
			case <-w.done:
				return
			}
		}
	}()
}

func NewWatcher() *Watcher {
	return &Watcher{}
}
