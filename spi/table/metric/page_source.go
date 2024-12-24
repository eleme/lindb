package metric

import (
	"context"
	"fmt"
	"slices"

	"github.com/samber/lo"

	"github.com/lindb/lindb/pkg/collections"
	"github.com/lindb/lindb/pkg/encoding"
	"github.com/lindb/lindb/series/field"
	"github.com/lindb/lindb/spi"
	"github.com/lindb/lindb/spi/types"
)

type PageSourceProvider struct{}

func NewPageSourceProvider() spi.PageSourceProvider {
	return &PageSourceProvider{}
}

func (p *PageSourceProvider) CreatePageSource(ctx context.Context) spi.PageSource {
	return &PageSource{
		ctx: ctx,
	}
}

type PageSource struct {
	ctx context.Context

	split *ScanSplit
}

func (mps *PageSource) AddSplit(split spi.Split) {
	if metricScanSplit, ok := split.(*ScanSplit); ok {
		mps.split = metricScanSplit
	}
}

func (mps *PageSource) GetNextPage() *types.Page {
	if mps.split == nil {
		return nil
	}

	defer func() {
		mps.split = nil
	}()

	if mps.split.tableScan.fields.Len() == 0 {
		// if fields is empty, then do series query.
		return mps.findSeries()
	}
	return mps.findSeriesData()
}

func (mps *PageSource) findSeriesData() *types.Page {
	tableScan := mps.split.tableScan
	familyLoaderMap := make(map[int64]*loader)
	for i := range mps.split.resultSet {
		rs := mps.split.resultSet[i]
		// check series ids if match
		dataLoader := rs.Load(mps.split.seriesIDHighKey, mps.split.lowSeriesIDs)
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
		return nil
	}

	familyLoaders := lo.Values(familyLoaderMap)
	// order by family time
	slices.SortFunc(familyLoaders, func(a, b *loader) int {
		return int(a.familyTime - b.familyTime)
	})

	isGrouping := tableScan.isGrouping()
	// executorPool := mps.split.tableScan.db.ExecutorPool()
	var (
		groupingAgg grouping
		tagsScanner *TagsScanner
	)
	if isGrouping {
		tagScanners := mps.split.groupingContext.BuildGroup(mps.split.seriesIDHighKey, mps.split.lowSeriesIDs)
		tagsScanner = NewTagsScanner(tagScanners)
		groupingAgg = newGroupingWithTags(tagsScanner, tableScan)
	} else {
		groupingAgg = newGroupingWithoutTags(tableScan)
	}

	columnRollups := tableScan.createRollups()
	numOfPoints := tableScan.timeRange.NumOfPoints(tableScan.interval)
	start := tableScan.timeRange.Start
	step := tableScan.interval.Int64()

	it := mps.split.lowSeriesIDs.PeekableIterator()
	// loop each low series ids
	for it.HasNext() {
		lowSeriesID := it.Next()
		// TODO: go?
		// executorPool.Grouping.Submit(mps.ctx, newBuildGrouping())
		aggregator := groupingAgg.GetAggregator(lowSeriesID)
		// TODO:: go?

		// load all fields data from families
		for _, loader := range familyLoaders {
			fmt.Printf("start load family time=%v\n", loader.familyTime)
			// aggregator.FillColumnStreams(familyIndex, columns)
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

	return mps.buildOutputPage(groupingAgg)
}

func (mps *PageSource) buildOutputPage(rs grouping) *types.Page {
	page := types.NewPage()
	var (
		fields          []*types.Column
		grouping        []*types.Column
		groupingIndexes []int
	)
	for idx, output := range mps.split.tableScan.outputs {
		column := types.NewColumn()
		page.AppendColumn(output, column)
		if lo.ContainsBy(mps.split.tableScan.fields, func(item field.Meta) bool {
			return item.Name.String() == output.Name
		}) {
			fields = append(fields, column)
		} else {
			grouping = append(grouping, column)
			groupingIndexes = append(groupingIndexes, idx)
		}
	}
	// set the indexes of grouping column
	page.SetGrouping(groupingIndexes)

	hasGrouping := mps.split.tableScan.isGrouping()
	rs.ForEach(func(tags *GroupingKey, rs []*collections.FloatArray) {
		if hasGrouping {
			tags := mps.split.tableScan.grouping.GetTagValues(*tags)
			for idx, tag := range tags {
				grouping[idx].AppendString(tag)
			}
		}
		for fieldIdx, stream := range rs {
			timeSeries := types.NewTimeSeriesWithValues(mps.split.tableScan.timeRange, mps.split.tableScan.interval, stream.Values())
			fields[fieldIdx].AppendTimeSeries(timeSeries)
		}
	})
	return page
}

func (mps *PageSource) findSeries() *types.Page {
	tagScanners := mps.split.groupingContext.BuildGroup(mps.split.seriesIDHighKey, mps.split.lowSeriesIDs)
	tagsScanner := NewTagsScanner(tagScanners)
	result := make(map[*GroupingKey]struct{})
	it := mps.split.lowSeriesIDs.PeekableIterator()
	for it.HasNext() {
		// loop each low series ids
		lowSeriesID := it.Next()
		// build grouping keys
		key := tagsScanner.FindTagValues(lowSeriesID)
		if _, ok := result[key]; !ok {
			fmt.Println("add.......")
			result[key.Clone()] = struct{}{}
		}
	}
	fmt.Printf("serie sgrouping...%v,result====%v\n", mps.split.tableScan.grouping.tags, result)

	mps.split.tableScan.grouping.CollectTagValueIDs(tagsScanner.GetTagValueIDs())
	mps.split.tableScan.grouping.CollectTagValues()

	page := types.NewPage()
	var (
		grouping        []*types.Column
		groupingIndexes []int
	)
	for idx, output := range mps.split.tableScan.outputs {
		column := types.NewColumn()
		page.AppendColumn(output, column)
		grouping = append(grouping, column)
		groupingIndexes = append(groupingIndexes, idx)
	}
	// set grouping index of the columns
	page.SetGrouping(groupingIndexes)

	// set tag values
	for tagValueIDs := range result {
		tags := mps.split.tableScan.grouping.GetTagValues(*tagValueIDs)
		for idx, tag := range tags {
			grouping[idx].AppendString(tag)
		}
	}
	return page
}
