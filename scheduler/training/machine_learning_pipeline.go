package training

import (
	"fmt"
	"time"

	"d7y.io/dragonfly/v2/scheduler/config"

	"d7y.io/dragonfly/v2/scheduler/storage"
)

type MachineLearning interface {
	Serve()
	Stop()
}

var MLStore map[config.MLType]struct{}

func init() {
	MLStore = make(map[config.MLType]struct{})
	MLStore[config.LinearMachineLearning] = struct{}{}
}

func NewML(mlType config.MLType, storage storage.Storage, refreshInterval time.Duration, cfg config.DynconfigInterface) (MachineLearning, error) {
	switch mlType {
	case config.LinearMachineLearning:
		return NewLinearTraining(storage, refreshInterval, cfg), nil
	}
	return nil, fmt.Errorf("unrecognized ml_type, type is %s", mlType)
}
