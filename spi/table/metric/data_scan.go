package metric

import (
	"fmt"
	"slices"

	"github.com/samber/lo"

	"github.com/lindb/lindb/pkg/collections"
	"github.com/lindb/lindb/pkg/encoding"
	"github.com/lindb/lindb/series/field"
)

type dataScan struct {
	inbound chan *DataSplit

	outbound chan<- *DataSplit
}

func NewDataScan(inbound, outbound chan *DataSplit) *dataScan {
	return &dataScan{
		inbound:  inbound,
		outbound: outbound,
	}
}

func (ds *dataScan) Start() {
	ds.inbound = make(chan *DataSplit)

	ds.outbound = (&reducer{}).inbound

	go func() {
		for split := range ds.inbound {
			ds.process(split)
		}
	}()
}

func (ds *dataScan) process(split *DataSplit) {
	if split.partition.tableScan.fields.Len() == 0 {
		// if fields is empty, then do series query(send reducer task).
		ds.outbound <- split
		return
	}

	// find time series data of filed
	tableScan := split.partition.tableScan
	familyLoaders := ds.buildFamilyLoaders(split)
	if len(familyLoaders) == 0 {
		// family not match
		return
	}

	isGrouping := tableScan.isGrouping()
	var (
		groupingAgg grouping
		tagsScanner *TagsScanner
	)

	if isGrouping {
		tagScanners := split.groupingContext.BuildGroup(split.seriesIDHighKey, split.lowSeriesIDs)
		tagsScanner = NewTagsScanner(tagScanners)
		groupingAgg = newGroupingWithTags(tagsScanner, tableScan)
	} else {
		groupingAgg = newGroupingWithoutTags(tableScan)
	}

	split.groupingAgg = groupingAgg

	columnRollups := tableScan.createRollups()
	numOfPoints := tableScan.timeRange.NumOfPoints(tableScan.interval)
	start := tableScan.timeRange.Start
	step := tableScan.interval.Int64()

	it := split.lowSeriesIDs.PeekableIterator()
	// loop each low series ids
	for it.HasNext() {
		lowSeriesID := it.Next()
		aggregator := groupingAgg.GetAggregator(lowSeriesID)

		// load all fields data from families
		for _, loader := range familyLoaders {
			fmt.Printf("start load family time=%v\n", loader.familyTime)
			// TODO: go? reset stream
			for _, family := range loader.families {
				slotRange := family.filterResultSet.SlotRange()

				family.loader.Load(lowSeriesID, func(field field.Meta, getter encoding.TSDValueGetter) {
					columnStream := loader.streams.GetStreamByIndex(field.Index)
					fn := field.Type.AggType().Aggregate
					for movingSourceSlot := slotRange.Start; movingSourceSlot <= slotRange.End; movingSourceSlot++ {
						value, ok := getter.GetValue(movingSourceSlot)
						if !ok {
							// no data, goto next loop
							continue
						}
						columnStream.SetAtStep(int(movingSourceSlot), value, fn)
					}
				})
			}
		}

		// do rollup(down sampling)/aggregation for each column
		for index, column := range tableScan.columns {
			field := column.meta

			// do rollup(down sampling)
			for _, loader := range familyLoaders {
				familyTime := loader.familyTime
				slotRange := loader.timeRange
				interval := loader.interval.Int64()
				for movingSourceSlot := slotRange.Start; movingSourceSlot <= slotRange.End; movingSourceSlot++ {
					timestamp := familyTime + int64(movingSourceSlot)*interval
					value := loader.streams.GetStreamByIndex(field.Index).GetAtStep(int(movingSourceSlot))

					// rollup
					for idx, r := range column.rollups {
						columnRollups[idx].doRollup(r.aggType, timestamp, value)
					}
				}
			}

			// do aggregation
			for aggIdx, agg := range column.aggs {
				idx := index + aggIdx
				if aggregator[idx] == nil {
					aggregator[idx] = collections.NewFloatArray(numOfPoints)
				}
				// fmt.Printf("rollup result=%v,%v\n", rollups[agg.target].timeseries.values, rollups[agg.target].timeseries.timestamps)
				// aggregate
				aggregate(agg.aggType, start, step, aggregator[idx], columnRollups[agg.target].timeseries)
				fmt.Printf("rollup result=%v,%v,%v\n", agg.target, columnRollups[agg.target].timeseries.values, aggregator[idx].Values())
			}

			// reset rollup context for next column
			for idx := range column.rollups {
				columnRollups[idx].reset()
			}
		}
	}

	// TODO: remove it?
	if isGrouping {
		tableScan.grouping.CollectTagValueIDs(tagsScanner.GetTagValueIDs())
		tableScan.grouping.CollectTagValues()
	}

	ds.outbound <- split
}

func (rs *dataScan) buildFamilyLoaders(split *DataSplit) []*loader {
	tableScan := split.partition.tableScan
	familyLoaderMap := make(map[int64]*loader)
	for i := range split.partition.resultSet {
		rs := split.partition.resultSet[i]
		// check series ids if match
		dataLoader := rs.Load(split.seriesIDHighKey, split.lowSeriesIDs)
		if dataLoader != nil {
			family, ok := familyLoaderMap[rs.FamilyTime()]
			if ok {
				family.families = append(family.families, &familyLoader{filterResultSet: rs, loader: dataLoader})
				family.timeRange = family.timeRange.Union(rs.SlotRange())
			} else {
				familyLoaderMap[rs.FamilyTime()] = &loader{
					familyTime: rs.FamilyTime(),
					interval:   rs.Interval(),
					families:   []*familyLoader{{filterResultSet: rs, loader: dataLoader}},
					timeRange:  rs.SlotRange(),
					streams:    newStreams(tableScan.fields.Len(), tableScan.db.GetStream),
				}
			}
		}
	}
	if len(familyLoaderMap) == 0 {
		// family not match
		return nil
	}

	familyLoaders := lo.Values(familyLoaderMap)
	// order by family time
	slices.SortFunc(familyLoaders, func(a, b *loader) int {
		return int(a.familyTime - b.familyTime)
	})
	return familyLoaders
}
