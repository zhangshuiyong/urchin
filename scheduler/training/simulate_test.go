package training

import (
	"d7y.io/dragonfly/v2/scheduler/storage"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestTraining(t *testing.T) {
	sto, _ := storage.New(os.TempDir())
	rand.Seed(time.Now().Unix())

	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T)
	}{
		{
			name:    "random record preprocess",
			baseDir: os.TempDir(),
			mock: func(t *testing.T) {
				for i := 0; i < 10100; i++ {
					record := storage.Record{
						IP:             rand.Intn(100)%2 + 1,
						HostName:       rand.Intn(100)%2 + 1,
						Tag:            rand.Intn(100)%2 + 1,
						Rate:           float64(rand.Intn(300) + 10),
						ParentPiece:    float64(rand.Intn(240) + 14),
						SecurityDomain: rand.Intn(100)%2 + 1,
						IDC:            rand.Intn(100)%2 + 1,
						NetTopology:    rand.Intn(100)%2 + 1,
						Location:       rand.Intn(100)%2 + 1,
						UploadRate:     float64(rand.Intn(550) + 3),
						State:          rand.Intn(4),
						CreateAt:       time.Now().Unix()/7200 + rand.Int63n(10),
						UpdateAt:       time.Now().Unix()/7200 + rand.Int63n(10),
						ParentCreateAt: time.Now().Unix()/7200 + rand.Int63n(10),
						ParentUpdateAt: time.Now().Unix()/7200 + rand.Int63n(10),
					}
					err := sto.Create(record)
					if err != nil {
						t.Fatal(err)
					}
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.mock(t)
			file, _ := os.Open("/tmp/record.csv")
			data, _ := New(file)
			instance, err := data.PreProcess()
			fmt.Println(instance)
			if err != nil {
				t.Fatal(err)
			}
			model, err := TrainProcess(instance, NewTrainOptions())
			if err != nil {
				t.Fatal(err)
			}
			predict, err := model.Predict(instance)
			if err != nil {
				t.Fatal(err)
			}
			evaluate, err := Evaluate(predict, instance)
			if err != nil {
				return
			}
			fmt.Println(evaluate)
		})
	}
}
