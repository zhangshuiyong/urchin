package training

import (
	"sync"

	"d7y.io/dragonfly/v2/scheduler/training/models"
	"github.com/sjwhitworth/golearn/base"
)

var once sync.Once

// TrainProcess fit and evaluate models for each parent peer.
func TrainProcess(instance *base.DenseInstances, to *TrainOptions, model *models.LinearRegression) error {
	once.Do(func() {
		model = models.NewLinearRegression()
	})
	err := model.Fit(instance, to.LearningRate)
	if err != nil {
		return err
	}
	return nil
}
