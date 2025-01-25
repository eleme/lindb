package metric

import (
	"context"
	"errors"
	"fmt"

	"github.com/samber/lo"

	"github.com/lindb/lindb/constants"
	"github.com/lindb/lindb/flow"
	"github.com/lindb/lindb/models"
	"github.com/lindb/lindb/series/field"
	"github.com/lindb/lindb/series/metric"
	"github.com/lindb/lindb/series/tag"
	"github.com/lindb/lindb/spi"
	"github.com/lindb/lindb/spi/types"
	"github.com/lindb/lindb/sql/tree"
	"github.com/lindb/lindb/tsdb"
)

type pageSourceConnectorProvider struct {
	engine tsdb.Engine
}

func NewPageSourceConnectorProvider(engine tsdb.Engine) spi.PageSourceConnectorProvider {
	return &pageSourceConnectorProvider{
		engine: engine,
	}
}

func (p *pageSourceConnectorProvider) CreatePageSourceConnector(ctx context.Context,
	table spi.TableHandle, partitionIDs []int,
	predicate tree.Expression,
	outputColumns []types.ColumnMetadata, assignments []*spi.ColumnAssignment,
) spi.PageSourceConnector {
	return &pageSourceConnector{
		ctx:           ctx,
		engine:        p.engine,
		table:         table,
		partitionIDs:  partitionIDs,
		predicate:     predicate,
		outputColumns: outputColumns,
		assignments:   assignments,
	}
}

type pageSourceConnector struct {
	ctx context.Context

	engine tsdb.Engine

	table         spi.TableHandle
	partitionIDs  []int
	predicate     tree.Expression
	outputColumns []types.ColumnMetadata
	assignments   []*spi.ColumnAssignment

	pages chan *types.Page
}

func (psc *pageSourceConnector) Open() {
	tableScan := psc.buildTableScan()
	if tableScan == nil {
		fmt.Println("table scan is nil")
		return
	}
	// find partitions
	partitions := psc.findPartitions(tableScan, psc.partitionIDs)
	if len(partitions) == 0 {
		return
	}

	tableScan.predicate = psc.predicate
	tableScan.lookupColumnValues()

	psc.pages = make(chan *types.Page)

	splitCh := make(chan *DataSplit)

	reduceCh := make(chan *DataSplit)

	for i := range partitions {
		partitionScan := NewPartitionScan(partitions[i], splitCh)
		partitionScan.Start()

		if tableScan.fields.Len() > 0 {
			dataScan := NewDataScan(splitCh, reduceCh)
			dataScan.Start()
		}
	}

	reducer := NewReducer(tableScan, reduceCh, psc.pages)
	reducer.Start()
}

func (psc *pageSourceConnector) GetPages() <-chan *types.Page {
	return psc.pages
}

func (psc *pageSourceConnector) buildTableScan() *TableScan {
	metricTable, ok := psc.table.(*TableHandle)
	if !ok {
		panic(fmt.Sprintf("metric provider not support table handle<%T>", psc.table))
	}
	db, ok := psc.engine.GetDatabase(metricTable.Database)
	if !ok {
		panic(fmt.Errorf("%w: %s", constants.ErrDatabaseNotFound, metricTable.Database))
	}
	// find table(metric) schema
	metricID, schema, err := psc.getSchema(db, metricTable)
	if err != nil {
		if errors.Is(err, constants.ErrNotFound) {
			return nil
		}
		// if isn't not found error, throw it
		panic(err)
	}
	// mapping fields for searching
	var fields field.Metas
	var columns []*column
	index := uint8(0)
	columnIndex := 0
	lo.ForEach(psc.outputColumns, func(columnMeta types.ColumnMetadata, _ int) {
		if fieldMeta, ok := lo.Find(schema.Fields, func(fieldMeta field.Meta) bool {
			return columnMeta.Name == fieldMeta.Name.String() && columnMeta.DataType != types.DTString
		}); ok {
			fieldMeta.Index = index
			fields = append(fields, fieldMeta)
			index++

			column := &column{meta: fieldMeta, offset: columnIndex}
			columns = append(columns, column)

			// find column handles for field
			columnHandles := lo.Filter(psc.assignments, func(item *spi.ColumnAssignment, index int) bool {
				return item.Column == fieldMeta.Name.String()
			})
			// not input aggregation func for this field
			if len(columnHandles) == 0 {
				// if column handle not found, set default aggregation using field aggregation type
				funcName := tree.FuncName(fieldMeta.Type.String()) // TODO: using same type
				column.handles = []*ColumnHandle{{Downsampling: funcName, Aggregation: funcName}}
			}
			for _, columnHandle := range columnHandles {
				if handle, ok := columnHandle.Handler.(*ColumnHandle); ok {
					column.handles = append(column.handles, handle)
					columnIndex++
				}
			}
		}
	})
	// mpaaing tags for grouping
	var groupingTags tag.Metas
	lo.ForEach(psc.outputColumns, func(column types.ColumnMetadata, _ int) {
		if tagKey, ok := lo.Find(schema.TagKeys, func(tagMeta tag.Meta) bool {
			return column.Name == tagMeta.Key && column.DataType == types.DTString
		}); ok {
			groupingTags = append(groupingTags, tagKey)
		}
	})
	fmt.Printf("all fields=%v, group key=%v, select field=%v,output=%v\n", schema.Fields, groupingTags, fields, psc.outputColumns)

	if len(fields)+len(groupingTags) != len(psc.outputColumns) {
		// TODO: only check grouping keys
		// output columns size not match
		return nil
	}

	var grouping *Grouping
	if len(groupingTags) > 0 {
		grouping = NewGrouping(db, groupingTags)
	}
	maxOfRollups := 0
	numOfAggs := 0
	for _, column := range columns {
		// init column rollup and aggregation context
		column.init()
		if maxOfRollups < len(column.rollups) {
			maxOfRollups = len(column.rollups)
		}
		numOfAggs += len(column.aggs)
	}

	return &TableScan{
		ctx:             psc.ctx,
		db:              db,
		schema:          schema,
		metricID:        metricID,
		timeRange:       metricTable.TimeRange,
		interval:        metricTable.Interval,
		storageInterval: metricTable.StorageInterval,
		fields:          fields,
		columns:         columns,
		maxOfRollups:    maxOfRollups,
		numOfAggs:       numOfAggs,
		grouping:        grouping,
		outputs:         psc.outputColumns,
	}
}

// getSchema returns table schema based on table handle.
func (psc *pageSourceConnector) getSchema(db tsdb.Database, table *TableHandle) (metric.ID, *metric.Schema, error) {
	// find metric id(table id)
	metricID, err := db.MetaDB().GetMetricID(table.Namespace, table.Metric)
	if err != nil {
		return 0, nil, err
	}
	// find table schema
	schema, err := db.MetaDB().GetSchema(metricID)
	if err != nil {
		return 0, nil, err
	}
	return metricID, schema, nil
}

func (psc *pageSourceConnector) findPartitions(tableScan *TableScan, partitionIDs []int) (partitions []*Partition) {
	for _, id := range partitionIDs {
		shard, ok := tableScan.db.GetShard(models.ShardID(id))
		if ok {
			families := shard.GetDataFamilies(tableScan.storageInterval.Type(), tableScan.timeRange)
			if len(families) > 0 {
				partitions = append(partitions, &Partition{
					shard:    shard,
					families: families,
				})
			}
		}
	}
	return
}

func (psc *pageSourceConnector) lookupColumnValues(tableScan *TableScan) {
	// lookup column if predicate not nil
	if psc.predicate != nil {
		tableScan.filterResult = make(map[tree.NodeID]*flow.TagFilterResult)
		lookup := NewColumnValuesLookVisitor(tableScan)
		_ = psc.predicate.Accept(nil, lookup)
	}
}
