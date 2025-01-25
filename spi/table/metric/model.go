package metric

import (
	"fmt"

	common_tileutil "github.com/lindb/common/pkg/timeutil"
	"github.com/lindb/roaring"

	"github.com/lindb/lindb/flow"
	"github.com/lindb/lindb/pkg/encoding"
	"github.com/lindb/lindb/pkg/timeutil"
	"github.com/lindb/lindb/spi"
	"github.com/lindb/lindb/spi/types"
	"github.com/lindb/lindb/sql/tree"
	"github.com/lindb/lindb/tsdb"
)

func init() {
	// register table/column handle
	encoding.RegisterNodeType(TableHandle{})
	encoding.RegisterNodeType(ColumnHandle{})

	spi.RegisterCreateTableFn(spi.Metric, func(db, ns, name string) spi.TableHandle {
		return &TableHandle{
			Database:  db,
			Namespace: ns,
			Metric:    name,
			// FIXME: remove it
			Interval:        timeutil.Interval(10 * common_tileutil.OneSecond),
			StorageInterval: timeutil.Interval(10 * common_tileutil.OneSecond),
			IntervalRatio:   1,
		}
	})

	spi.RegisterApplyAggregationFn(spi.Metric, func(table spi.TableHandle, tableMeta *types.TableMetadata, aggregations []spi.ColumnAggregation) *spi.ApplyAggregationResult {
		result := &spi.ApplyAggregationResult{}
		// FIXME: find downSampling agg
		for _, agg := range aggregations {
			result.ColumnAssignments = append(result.ColumnAssignments,
				&spi.ColumnAssignment{Column: agg.Column, Handler: &ColumnHandle{Downsampling: tree.Max, Aggregation: agg.AggFuncName}},
			)
		}
		return result
	})
}

type TableHandle struct {
	Database        string             `json:"database"`
	Namespace       string             `json:"namespace"`
	Metric          string             `json:"metric"`
	TimeRange       timeutil.TimeRange `json:"timeRange"`
	Interval        timeutil.Interval  `json:"interval"`
	StorageInterval timeutil.Interval  `json:"storageInterval"`
	IntervalRatio   int                `json:"intervalRatio"`
}

func (t *TableHandle) SetTimeRange(timeRange timeutil.TimeRange) {
	t.TimeRange = timeRange
}

func (t *TableHandle) GetTimeRange() timeutil.TimeRange {
	return t.TimeRange
}

func (t *TableHandle) Kind() spi.DatasourceKind {
	return spi.Metric
}

func (t *TableHandle) String() string {
	return fmt.Sprintf("%s:%s:%s", t.Database, t.Namespace, t.Metric)
}

type ColumnHandle struct {
	Downsampling tree.FuncName `json:"downsampling"`
	Aggregation  tree.FuncName `json:"aggregation"`
}

type ScanSplit struct {
	tableScan       *TableScan
	groupingContext flow.GroupingContext
	resultSet       []flow.FilterResultSet // FIXME: need close result set after task finish

	lowSeriesIDs    roaring.Container
	seriesIDHighKey uint16
}

type DataSplit struct {
	partition       *Partition
	groupingContext flow.GroupingContext
	groupingAgg     grouping

	seriesIDHighKey uint16
	lowSeriesIDs    roaring.Container
}

type Partition struct {
	tableScan *TableScan
	shard     tsdb.Shard
	families  []tsdb.DataFamily

	resultSet []flow.FilterResultSet
}

type TimeSeries struct {
	timestamps []int64
	values     []float64
}

func newTimeSeries(capacity int) *TimeSeries {
	return &TimeSeries{
		timestamps: make([]int64, 0, capacity),
		values:     make([]float64, 0, capacity),
	}
}

func (ts *TimeSeries) Append(timestamp int64, value float64) {
	ts.timestamps = append(ts.timestamps, timestamp)
	ts.values = append(ts.values, value)
}

func (ts *TimeSeries) Reset() {
	ts.timestamps = ts.timestamps[:0]
	ts.values = ts.values[:0]
}
