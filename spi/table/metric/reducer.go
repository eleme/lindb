package metric

import (
	"fmt"

	"github.com/samber/lo"

	"github.com/lindb/lindb/pkg/collections"
	"github.com/lindb/lindb/series/field"
	"github.com/lindb/lindb/spi/types"
)

type reducer struct {
	tableScan *TableScan
	inbound   <-chan *DataSplit
	outbound  chan<- *types.Page

	result map[*GroupingKey][]*collections.FloatArray // tags => series data of fields(aggregators)
}

func NewReducer(tableScan *TableScan, inbound <-chan *DataSplit, outbound chan<- *types.Page) *reducer {
	return &reducer{
		tableScan: tableScan,
		inbound:   inbound,
		result:    make(map[*GroupingKey][]*collections.FloatArray),
	}
}

func (r *reducer) Start() {
	go func() {
		for {
			select {
			case <-r.tableScan.ctx.Done():
				return
			case split := <-r.inbound:
				r.process(split)
			}
		}
	}()
}

func (r *reducer) process(split *DataSplit) {
	if r.tableScan.fields.Len() == 0 {
		// find series data
		r.findSeries(split)
	}
	r.tableScan.grouping.CollectTagValues()

	r.outbound <- r.buildOutputPage(split)
}

func (r *reducer) findSeries(split *DataSplit) {
	tagScanners := split.groupingContext.BuildGroup(split.seriesIDHighKey, split.lowSeriesIDs)
	tagsScanner := NewTagsScanner(tagScanners)
	it := split.lowSeriesIDs.PeekableIterator()
	for it.HasNext() {
		// loop each low series ids
		lowSeriesID := it.Next()
		// build grouping keys
		key := tagsScanner.FindTagValues(lowSeriesID)
		if _, ok := r.result[key]; !ok {
			r.result[key.Clone()] = nil
		}
	}
	tableScan := split.partition.tableScan
	fmt.Printf("serie sgrouping...%v,result====%v\n", tableScan.grouping.tags)

	tableScan.grouping.CollectTagValueIDs(tagsScanner.GetTagValueIDs())
}

func (r *reducer) buildOutputPage(split *DataSplit) *types.Page {
	page := types.NewPage()
	var (
		fields          []*types.Column
		grouping        []*types.Column
		groupingIndexes []int
	)
	for idx, output := range r.tableScan.outputs {
		column := types.NewColumn()
		page.AppendColumn(output, column)
		if lo.ContainsBy(r.tableScan.fields, func(item field.Meta) bool {
			return item.Name.String() == output.Name
		}) {
			fields = append(fields, column)
		} else {
			grouping = append(grouping, column)
			groupingIndexes = append(groupingIndexes, idx)
		}
	}
	// set grouping index of the columns
	page.SetGrouping(groupingIndexes)

	hasGrouping := r.tableScan.isGrouping()
	// set tag values
	for tags, seriesData := range r.result {
		if hasGrouping {
			tags := r.tableScan.grouping.GetTagValues(*tags)
			for idx, tag := range tags {
				grouping[idx].AppendString(tag)
			}
		}
		for fieldIdx, stream := range seriesData {
			timeSeries := types.NewTimeSeriesWithValues(r.tableScan.timeRange, r.tableScan.interval, stream.Values())
			fields[fieldIdx].AppendTimeSeries(timeSeries)
		}
	}
	return page
}
