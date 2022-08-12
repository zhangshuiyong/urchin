package training

import (
	"d7y.io/dragonfly/v2/scheduler/training/models"
	"fmt"
	"github.com/sjwhitworth/golearn/base"
)

// TrainProcess fit and evaluate models for each parent peer.
func TrainProcess(instance *base.DenseInstances, to *TrainOptions) (*models.LinearRegression, error) {
	model := models.NewLinearRegression()

	err := model.Fit(instance, to.LearningRate)
	if err != nil {
		return nil, err
	}
	fmt.Println(model.RegressionCoefficients)
	return model, nil
}
