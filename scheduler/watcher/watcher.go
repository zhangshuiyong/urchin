package watcher

import (
	"context"
	"sort"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client"
)

type Watcher struct {
	needVersion  chan uint64
	modelVersion chan *types.ModelVersion
	done         chan struct{}
	mc           client.Client
	standard     *types.ModelVersion
}

func (w *Watcher) Serve() {
	go func() {
		for {
			select {
			case schID := <-w.needVersion:
				if schID <= 0 {
					w.modelVersion <- nil
					continue
				}
				versions, err := w.mc.ListModelVersions(context.Background(), &managerv1.ListModelVersionsRequest{
					ModelId:     types.ModelIDEvaluator,
					SchedulerId: schID,
				})
				if err != nil {
					w.modelVersion <- nil
					continue
				}
				sort.Slice(versions, func(i, j int) bool {
					return versions.ModelVersions[i].UpdatedAt.Seconds > versions.ModelVersions[i].UpdatedAt.Seconds
				})
				flag := false
				for _, version := range versions.ModelVersions {
					if (w.standard != nil && w.satisfyStandard(version)) || (w.standard == nil) {
						w.modelVersion <- &types.ModelVersion{
							Data: version.Data,
							MAE:  version.Mae,
							MSE:  version.Mse,
							RMSE: version.Rmse,
							R2:   version.R2,
						}
						flag = true
						break
					}
				}

				if !flag {
					w.modelVersion <- nil
				}
			case <-w.done:
				return
			}
		}
	}()
}

func (w *Watcher) satisfyStandard(version *managerv1.ModelVersion) bool {
	if version.Mae < w.standard.MAE || version.Mse < w.standard.MSE || version.Rmse < w.standard.RMSE || version.R2 < w.standard.R2 {
		return false
	}
	return true
}

type WatcherOptionFunc func(options *Watcher)

func WithStandard(standard *types.ModelVersion) WatcherOptionFunc {
	return func(options *Watcher) {
		options.standard = standard
	}
}

func (w *Watcher) Stop() {
	close(w.done)
}

func NewWatcher(mc client.Client, nv chan uint64, mv chan *types.ModelVersion, options ...WatcherOptionFunc) *Watcher {
	w := &Watcher{
		needVersion:  nv,
		modelVersion: mv,
		done:         make(chan struct{}),
		mc:           mc,
	}
	for _, opts := range options {
		opts(w)
	}
	return w
}
