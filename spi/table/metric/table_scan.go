package metric

import (
	"context"
	"fmt"

	"github.com/lindb/roaring"
	"github.com/samber/lo"

	"github.com/lindb/lindb/constants"
	"github.com/lindb/lindb/flow"
	"github.com/lindb/lindb/pkg/timeutil"
	"github.com/lindb/lindb/series/field"
	"github.com/lindb/lindb/series/metric"
	"github.com/lindb/lindb/spi"
	"github.com/lindb/lindb/spi/types"
	"github.com/lindb/lindb/sql/expression"
	"github.com/lindb/lindb/sql/tree"
	"github.com/lindb/lindb/tsdb"
)

type column struct {
	offset  int
	meta    field.Meta
	handles []*ColumnHandle

	rollups []rollupConfig
	aggs    []aggConfig
}

func (c *column) init() {
	rollupMap := make(map[tree.FuncName]struct{})
	index := 0
	for _, handle := range c.handles {
		c.aggs = append(c.aggs, aggConfig{target: index, aggType: getAggFunc(handle.Aggregation)})
		if _, ok := rollupMap[handle.Downsampling]; !ok {
			c.rollups = append(c.rollups, rollupConfig{aggType: getAggFunc(handle.Downsampling)})
			index++
		}
	}
}

type rollupConfig struct {
	aggType field.AggType
}

type aggConfig struct {
	target  int // ref to rollup result set
	aggType field.AggType
}

type TableScan struct {
	ctx       context.Context
	db        tsdb.Database
	schema    *metric.Schema
	predicate tree.Expression
	grouping  *Grouping

	// TODO: check if found all filter column values
	filterResult map[tree.NodeID]*flow.TagFilterResult

	fields       field.Metas
	columns      []*column
	maxOfRollups int
	numOfAggs    int
	outputs      []types.ColumnMetadata
	assignments  []*spi.ColumnAssignment

	timeRange       timeutil.TimeRange
	interval        timeutil.Interval
	storageInterval timeutil.Interval
	metricID        metric.ID
}

func (t *TableScan) isGrouping() bool {
	return t.grouping != nil && t.grouping.tags.Len() > 0
}

func (t *TableScan) createRollups() (rs rollups) {
	rs = make(rollups, t.maxOfRollups)
	for index := range rs {
		rs[index] = newRollup(t.timeRange.NumOfPoints(t.interval), t.interval.Int64())
	}
	return
}

func (t *TableScan) lookupColumnValues() {
	if t.predicate != nil {
		// lookup column if predicate not nil
		lookup := NewColumnValuesLookVisitor(t)
		_ = t.predicate.Accept(nil, lookup)
		// TODO: check filter result if empty????
	}
}

type ColumnValuesLookupVisitor struct {
	evalCtx   expression.EvalContext
	tableScan *TableScan
}

func NewColumnValuesLookVisitor(tableScan *TableScan) *ColumnValuesLookupVisitor {
	return &ColumnValuesLookupVisitor{
		tableScan: tableScan,
		evalCtx:   expression.NewEvalContext(tableScan.ctx),
	}
}

func (v *ColumnValuesLookupVisitor) Visit(context any, n tree.Node) any {
	var (
		column tree.Expression
		fn     func(columnName string) tree.Expr
	)
	switch node := n.(type) {
	case *tree.ComparisonExpression:
		columnValue, _ := expression.EvalString(v.evalCtx, node.Right)
		column = node.Left
		fn = func(columnName string) tree.Expr {
			return &tree.EqualsExpr{
				Name:  columnName,
				Value: columnValue,
			}
		}
	case *tree.InPredicate:
		var values []string
		if inListExpression, ok := node.ValueList.(*tree.InListExpression); ok {
			values = lo.Map(inListExpression.Values, func(item tree.Expression, index int) string {
				columnValue, _ := expression.EvalString(v.evalCtx, item)
				return columnValue
			})
		}
		column = node.Value
		fn = func(columnName string) tree.Expr {
			return &tree.InExpr{
				Name:   columnName,
				Values: values,
			}
		}
	case *tree.LikePredicate:
		columnValue, _ := expression.EvalString(v.evalCtx, node.Pattern)
		column = node.Value
		fn = func(columnName string) tree.Expr {
			return &tree.LikeExpr{
				Name:  columnName,
				Value: columnValue,
			}
		}
	case *tree.RegexPredicate:
		regexp, _ := expression.EvalString(v.evalCtx, node.Pattern)
		column = node.Value
		fn = func(columnName string) tree.Expr {
			return &tree.RegexExpr{
				Name:   columnName,
				Regexp: regexp,
			}
		}
	case *tree.NotExpression:
		return node.Value.Accept(context, v)
	case *tree.LogicalExpression:
		for _, term := range node.Terms {
			term.Accept(context, v)
		}
		return nil
	case *tree.Cast:
		return node.Expression.Accept(context, v)
	default:
		panic(fmt.Sprintf("column values lookup error, not support node type: %T", n))
	}
	// visit predicate which finding tag value ids
	return v.visitPredicate(n, column, fn)
}

func (v *ColumnValuesLookupVisitor) visitPredicate(predicate tree.Node, column tree.Expression,
	buildExpr func(columnName string) tree.Expr,
) (r any) {
	columnName, _ := expression.EvalString(v.evalCtx, column)

	tagMeta, ok := v.tableScan.schema.TagKeys.Find(columnName)
	if !ok {
		panic(fmt.Errorf("%w, column name: %s", constants.ErrColumnNotFound, columnName))
	}
	tagKeyID := tagMeta.ID
	var tagValueIDs *roaring.Bitmap
	var err error
	tagValueIDs, err = v.tableScan.db.MetaDB().FindTagValueDsByExpr(tagKeyID, buildExpr(columnName))
	if err != nil {
		panic(err)
	}

	if tagValueIDs == nil || tagValueIDs.IsEmpty() {
		panic(fmt.Errorf("%w, column name: %s", constants.ErrColumnValueNotFound, columnName))
	}

	v.tableScan.filterResult[predicate.GetID()] = &flow.TagFilterResult{
		TagKeyID:    tagKeyID,
		TagValueIDs: tagValueIDs,
	}
	return nil
}
