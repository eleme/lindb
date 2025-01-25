package metric

import (
	"errors"
	"fmt"

	"github.com/lindb/roaring"

	"github.com/lindb/lindb/constants"
	"github.com/lindb/lindb/flow"
)

type partitionScan struct {
	partition *Partition
	outbound  chan<- *DataSplit
}

func NewPartitionScan(partition *Partition, outbound chan<- *DataSplit) *partitionScan {
	return &partitionScan{
		partition: partition,
		outbound:  outbound,
	}
}

func (ps *partitionScan) Start() {
	go func() {
		ps.process(ps.partition)
	}()
}

func (ps *partitionScan) process(partition *Partition) {
	seriesIDs := ps.matchSeriesIDs(partition)
	var groupingContext flow.GroupingContext

	if seriesIDs.IsEmpty() {
		panic(constants.ErrSeriesIDNotFound)
	}

	tableScan := partition.tableScan
	if tableScan.isGrouping() {
		// if it has grouping, do group by tag keys, else just split series ids as batch first.
		seriesIDsAfterGrouping, groupingContext, err := partition.shard.IndexDB().
			GetGroupingContext(tableScan.grouping.tags, seriesIDs)
		if err != nil && !errors.Is(err, constants.ErrNotFound) {
			// TODO: add not found check
			panic(err)
		}
		// maybe filtering some series ids after grouping that is result of filtering.
		// if not found, return empty series ids.
		seriesIDs = seriesIDsAfterGrouping
		groupingContext = groupingContext
	}
	fmt.Printf("final series id=%s\n", seriesIDs)

	highKeys := seriesIDs.GetHighKeys()
	for index, highKey := range highKeys {
		ps.outbound <- &DataSplit{
			partition:       partition,
			groupingContext: groupingContext,

			seriesIDHighKey: highKey,
			lowSeriesIDs:    seriesIDs.GetContainerAtIndex(index),
		}
	}
}

func (ps *partitionScan) matchSeriesIDs(partition *Partition) *roaring.Bitmap {
	tableScan := partition.tableScan
	fmt.Printf("families=%v,fields=%v\n", partition.families, tableScan.fields)
	if tableScan.fields.Len() == 0 && len(partition.families) == 0 {
		// no data family return empty series ids
		return roaring.New()
	}

	seriesIDs := ps.lookupSeriesIDs(partition)
	result := roaring.New()

	for i := range partition.families {
		family := partition.families[i]
		// check family data if matches condition(series ids)
		resultSet, err := family.Filter(&flow.MetricScanContext{
			MetricID:  tableScan.metricID,
			SeriesIDs: seriesIDs,
			Fields:    tableScan.fields,
			TimeRange: tableScan.timeRange,
			Interval:  family.Interval(),
		})

		if err != nil && !errors.Is(err, constants.ErrNotFound) {
			panic(err)
		}

		for i := range resultSet {
			rs := resultSet[i]

			// check double, maybe some series ids be filtered out when do grouping.
			finalSeriesIDs := roaring.FastAnd(seriesIDs, rs.SeriesIDs())
			if finalSeriesIDs.IsEmpty() {
				continue
			}

			partition.resultSet = append(partition.resultSet, rs)
			result.Or(finalSeriesIDs)
		}
	}
	return result
}

func (ps *partitionScan) lookupSeriesIDs(partition *Partition) *roaring.Bitmap {
	var (
		seriesIDs *roaring.Bitmap
		err       error
		ok        bool
	)
	tableScan := partition.tableScan
	predicate := tableScan.predicate

	if predicate == nil {
		// if predicate nil, find all series ids under metric
		seriesIDs, err = partition.shard.IndexDB().GetSeriesIDsForMetric(tableScan.metricID)
		if err != nil {
			panic(err)
		}
	} else {
		// find series ids based on where condition
		lookup := NewRowLookupVisitor(partition)
		if seriesIDs, ok = predicate.Accept(nil, lookup).(*roaring.Bitmap); !ok {
			panic(constants.ErrSeriesIDNotFound)
		}
	}

	if seriesIDs == nil || seriesIDs.IsEmpty() {
		panic(constants.ErrSeriesIDNotFound)
	}
	return seriesIDs
}
