package scan

import (
	"context"
	"fmt"

	"github.com/lindb/lindb/constants"
	"github.com/lindb/lindb/spi"
	"github.com/lindb/lindb/spi/types"
	"github.com/lindb/lindb/sql/execution/pipeline/operator"
	"github.com/lindb/lindb/sql/planner/plan"
)

type TableScanOperatorFactory struct {
	conector spi.PageSourceConnector

	sourceID plan.PlanNodeID
}

func NewTableScanOperatorFactory(sourceID plan.PlanNodeID, connector spi.PageSourceConnector) operator.OperatorFactory {
	return &TableScanOperatorFactory{
		sourceID: sourceID,
		conector: connector,
	}
}

func (fct *TableScanOperatorFactory) CreateOperator(ctx context.Context) operator.Operator {
	return NewTableScanOperator(fct.sourceID, fct.conector)
}

type TableScanOperator struct {
	connector spi.PageSourceConnector

	sourceID plan.PlanNodeID
}

func NewTableScanOperator(sourceID plan.PlanNodeID, connector spi.PageSourceConnector) operator.SourceOperator {
	return &TableScanOperator{
		sourceID:  sourceID,
		connector: connector,
	}
}

func (op *TableScanOperator) GetSourceID() plan.PlanNodeID {
	return op.sourceID
}

func (op *TableScanOperator) NoMoreSplits() {
}

func (op *TableScanOperator) AddSplit(split spi.Split) {
	panic("remove it")
}

// AddInput implements operator.Operator
func (op *TableScanOperator) AddInput(page *types.Page) {
	panic(fmt.Errorf("%w: table scan cannot take input", constants.ErrNotSupportOperation))
}

// Finish implements operator.Operator
func (op *TableScanOperator) Finish() {
}

// GetOutput implements operator.Operator
func (op *TableScanOperator) GetOutput() *types.Page {
	// return op.pageSource.GetNextPage()
	return nil
}

func (op *TableScanOperator) GetOutbound() <-chan *types.Page {
	return op.connector.GetPages()
}

// IsFinished implements operator.Operator
func (op *TableScanOperator) IsFinished() bool {
	return true
}
