package training

import (
	"bufio"
	"bytes"
	"errors"
	"io"

	"github.com/sjwhitworth/golearn/base"
)

// Data Train provides training functions.
type Data struct {
	Reader io.ReadCloser
	// currentRecordLine capacity of lines which training has.
	CurrentRecordLine int
	Options           *DataOptions
}

// New return a Training instance.
func New(reader io.ReadCloser, option ...DataOptionFunc) (*Data, error) {
	t := &Data{
		Reader: reader,
		Options: &DataOptions{
			MaxBufferLine: DefaultMaxBufferLine,
			MaxRecordLine: DefaultMaxRecordLine,
		},
	}
	for _, o := range option {
		o(t.Options)
	}

	return t, nil
}

// LoadRecord read record from file and transform it to instance.
func (d *Data) LoadRecord(reader io.ReadCloser) (*base.DenseInstances, error) {
	if d.CurrentRecordLine < d.Options.MaxRecordLine {
		r := bufio.NewReader(reader)
		buf := new(bytes.Buffer)
		for i := 0; i < d.Options.MaxBufferLine; i++ {
			line, err := r.ReadString('\n')
			if err != nil && err != io.EOF {
				return nil, err
			}

			if err == io.EOF {
				break
			}
			buf.Write([]byte(line))
			d.CurrentRecordLine += 1
		}
		if buf.Len() == 0 {
			return nil, errors.New("file empty")
		}

		strReader := bytes.NewReader(buf.Bytes())
		instance, err := base.ParseCSVToInstancesFromReader(strReader, false)
		if err != nil {
			return nil, err
		}
		return instance, nil
	}
	return nil, nil
}

// PreProcess load and clean data before training.
func (d *Data) PreProcess() (*base.DenseInstances, error) {
	reader := d.Reader
	instance, err := d.LoadRecord(reader)
	if err != nil {
		if err.Error() == "file empty" {
			return nil, nil
		}
		return nil, err
	}
	// Change to Zero, for next loop
	d.CurrentRecordLine = 0
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
