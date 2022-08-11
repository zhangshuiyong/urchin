package training

type TrainOptions struct {
	LearningRate float64
	TestPercent  float64
}

type TrainOptionFunc func(options *TrainOptions)

func WithLearningRate(LearningRate float64) TrainOptionFunc {
	return func(options *TrainOptions) {
		options.LearningRate = LearningRate
	}
}

func WithTestPercent(TestPercent float64) TrainOptionFunc {
	return func(options *TrainOptions) {
		options.TestPercent = TestPercent
	}
}

func NewTrainOptions() *TrainOptions {
	return &TrainOptions{
		LearningRate: DefaultLearningRate,
		TestPercent:  TestSetPercent,
	}
}
