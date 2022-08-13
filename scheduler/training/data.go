package training

import (
	"io"

	"github.com/sjwhitworth/golearn/base"
)

// Data Train provides training functions.
type Data struct {
	Reader io.ReadCloser
	// currentRecordLine capacity of lines which training has.
	TotalDataRecordLine int64
	TotalTestRecordLine int64
	Options             *DataOptions
}

// New return a Training instance.
func New(reader io.ReadCloser, option ...DataOptionFunc) (*Data, error) {
	t := &Data{
		Reader: reader,
		Options: &DataOptions{
			MaxBufferLine: DefaultMaxBufferLine,
			MaxRecordLine: DefaultMaxRecordLine,
			TestPercent:   TestSetPercent,
		},
	}
	for _, o := range option {
		o(t.Options)
	}

	return t, nil
}

func (d *Data) Min(maxRecord int, totalRecord int64) int {
	if int64(maxRecord) > totalRecord {
		d.TotalDataRecordLine = 0
		return int(totalRecord)
	}
	d.TotalTestRecordLine -= int64(maxRecord)
	return maxRecord
}

// PreProcess load and clean data before training.
func (d *Data) PreProcess() (*base.DenseInstances, error) {
	// TODO
	loopTimes := d.Min(d.Options.MaxRecordLine, d.TotalDataRecordLine)
	instance, err := LoadRecord(d.Reader, loopTimes)
	if err != nil {
		return nil, err
	}

	err = MissingValue(instance)
	if err != nil {
		return nil, err
	}

	err = Normalize(instance, false)
	if err != nil {
		return nil, err
	}
	return instance, nil
}
