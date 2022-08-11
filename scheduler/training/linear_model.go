package training

import "d7y.io/dragonfly/v2/scheduler/training/models"

type LinearModel struct {
	// Model actual model.
	Model *models.LinearRegression
	Ev    *Eval
}
