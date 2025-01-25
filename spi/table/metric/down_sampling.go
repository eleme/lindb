package metric

import (
	"github.com/lindb/lindb/flow"
	"github.com/lindb/lindb/pkg/timeutil"
)

type familyLoader struct {
	filterResultSet flow.FilterResultSet
	loader          flow.DataLoader
}

type familyStream struct {
	fields []flow.Stream
}

type loader struct {
	interval timeutil.Interval // storage interval
	families []*familyLoader

	familyTime int64
	timeRange  timeutil.SlotRange // time range

	streams *Streams
}

type Streams struct {
	createStream func() flow.Stream

	streams []flow.Stream // stream of fields
}

func newStreams(fieldSize int, createStream func() flow.Stream) *Streams {
	return &Streams{
		createStream: createStream,
		streams:      make([]flow.Stream, fieldSize),
	}
}

func (s *Streams) GetStreamByIndex(field uint8) flow.Stream {
	stream := s.streams[field]
	if stream == nil {
		stream = s.createStream()
		s.streams[field] = stream
	}
	return stream
}

type stream struct {
	values []float64
}

func newStream(size int) flow.Stream {
	return &stream{
		values: make([]float64, size),
	}
}

func (s *stream) SetAtStep(step int, value float64, fn func(a, b float64) float64) {
	s.values[step] = fn(s.values[step], value)
}

func (s *stream) GetAtStep(step int) float64 {
	return s.values[step]
}

func (s *stream) Merge(stream flow.Stream, fn func(a, b float64) float64) {
	panic("not implemented")
}

func (s *stream) Values() []float64 {
	return s.values
}
