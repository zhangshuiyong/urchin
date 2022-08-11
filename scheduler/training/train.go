package training

import (
	"d7y.io/dragonfly/v2/scheduler/training/models"
	"github.com/sjwhitworth/golearn/base"
)

// TrainProcess fit and evaluate models for each parent peer.
func TrainProcess(trainMap map[float64]*base.DenseInstances, to *TrainOptions, modelMap map[float64]*models.LinearRegression) error {
	for key, value := range trainMap {
		if _, ok := modelMap[key]; !ok {
			model := models.NewLinearRegression()
			modelMap[key] = model
		}

		model := modelMap[key]
		train, _ := base.InstancesTrainTestSplit(value, to.TestPercent)
		err := model.Fit(train, to.LearningRate)
		if err != nil {
			return err
		}
	}
	return nil
}
