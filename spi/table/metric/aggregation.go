package metric

import (
	"fmt"

	"github.com/lindb/lindb/pkg/collections"
	"github.com/lindb/lindb/series/field"
)

func aggregate(fn field.AggType, start, interval int64, dst *collections.FloatArray, src *TimeSeries) {
	for index, timestamp := range src.timestamps {
		value := src.values[index]
		offset := int((timestamp - start) / interval)
		// TODO: add log
		if offset < 0 {
			fmt.Printf("warn offset < 0, offset=%v\n", offset)
		}

		if dst.HasValue(offset) {
			dst.SetValue(offset, fn.Aggregate(dst.GetValue(offset), value))
		} else {
			dst.SetValue(offset, value)
		}
	}
}
