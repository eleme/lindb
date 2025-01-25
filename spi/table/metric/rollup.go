package metric

import "github.com/lindb/lindb/series/field"

type rollups []*rollup

type rollup struct {
	timeseries *TimeSeries

	window        int64
	currTimestamp int64
	nextTimestamp int64
	currValue     float64
}

func newRollup(capacity int, window int64) *rollup {
	return &rollup{
		timeseries: newTimeSeries(capacity),
		window:     window,
	}
}

func (r *rollup) doRollup(aggType field.AggType, timestamp int64, value float64) {
	if r.currTimestamp == 0 {
		r.nextWindow(timestamp, value)
	} else if timestamp < r.nextTimestamp {
		r.currValue = aggType.Aggregate(r.currValue, value)
	} else {
		r.timeseries.Append(timestamp, value)

		r.nextWindow(timestamp, value)
	}
}

func (r *rollup) nextWindow(timestamp int64, value float64) {
	r.currTimestamp = timestamp
	r.currValue = value
	r.nextTimestamp = timestamp + r.window
}

func (r *rollup) reset() {
	r.window = 0
	r.currTimestamp = 0

	r.timeseries.Reset()
}
