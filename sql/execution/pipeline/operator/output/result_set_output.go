package output

import (
	"context"
	"fmt"

	"github.com/lindb/common/pkg/encoding"

	"github.com/lindb/lindb/spi/types"
	"github.com/lindb/lindb/sql/execution/buffer"
	"github.com/lindb/lindb/sql/execution/pipeline/operator"
	"github.com/lindb/lindb/sql/planner/plan"
)

type RSOutputOperatorFactory struct {
	output       buffer.OutputBuffer
	sourceLayout map[string]int
	columnNames  []string
	layout       []*plan.Symbol
	rebuildPage  bool
}

func NewRSOutputOperatorFactory(output buffer.OutputBuffer, columnNames []string, layout []*plan.Symbol, sourceLayout map[string]int) operator.OperatorFactory {
	rebuildPage := false
	for idx, symbol := range layout {
		sourceIdx, ok := sourceLayout[symbol.Name]
		if ok && (idx != sourceIdx || (len(columnNames) > 0 && columnNames[idx] != symbol.Name)) {
			rebuildPage = true
			break
		}
	}
	return &RSOutputOperatorFactory{
		output:       output,
		columnNames:  columnNames,
		layout:       layout,
		sourceLayout: sourceLayout,
		rebuildPage:  rebuildPage,
	}
}

// CreateOperator implements operator.OperatorFactory
func (fct *RSOutputOperatorFactory) CreateOperator(ctx context.Context) operator.Operator {
	return &ResultSetOutputOperator{
		output:       fct.output,
		sourceLayout: fct.sourceLayout,
		columnNames:  fct.columnNames,
		layout:       fct.layout,
		rebuildPage:  fct.rebuildPage,
	}
}

type ResultSetOutputOperator struct {
	output       buffer.OutputBuffer
	columnNames  []string
	sourceLayout map[string]int
	layout       []*plan.Symbol
	rebuildPage  bool
}

// AddInput implements operator.Operator
func (op *ResultSetOutputOperator) AddInput(page *types.Page) {
	if page == nil || page.NumRows() == 0 {
		return
	}
	if op.rebuildPage {
		targetPage := types.NewPage()
		for colIdx, col := range op.layout {
			if idx, ok := op.sourceLayout[col.Name]; ok {
				column := page.Layout[idx]
				if len(op.columnNames) > 0 {
					column.Name = op.columnNames[colIdx]
				}
				targetPage.AppendColumn(column, page.Columns[idx])
			}
		}
		fmt.Printf("after result set output rebuild, page=%v target=%v\n",
			string(encoding.JSONMarshal(page)), string(encoding.JSONMarshal(targetPage)))
		op.output.AddPage(targetPage)
	} else {
		op.output.AddPage(page)
	}
}

// Finish implements operator.Operator
func (op *ResultSetOutputOperator) Finish() {
	panic("unimplemented")
}

// GetOutput implements operator.Operator
func (op *ResultSetOutputOperator) GetOutput() *types.Page {
	return nil
}

// IsFinished implements operator.Operator
func (op *ResultSetOutputOperator) IsFinished() bool {
	panic("unimplemented")
}
