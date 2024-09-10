package operator

import (
	"fmt"

	"github.com/lindb/lindb/spi"
	"github.com/lindb/lindb/spi/types"
	"github.com/lindb/lindb/sql/expression"
	"github.com/lindb/lindb/sql/planner/plan"
)

type ProjectionOperatorFactory struct {
	project      *plan.ProjectionNode
	sourceLayout []*plan.Symbol
}

func NewProjectionOperatorFactory(project *plan.ProjectionNode, sourceLayout []*plan.Symbol) OperatorFactory {
	return &ProjectionOperatorFactory{
		project:      project,
		sourceLayout: sourceLayout,
	}
}

// CreateOperator implements OperatorFactory.
func (fct *ProjectionOperatorFactory) CreateOperator() Operator {
	return NewProjectionOperator(fct.project, fct.sourceLayout)
}

type ProjectionOperator struct {
	project *plan.ProjectionNode
	source  *spi.Page // TODO: refact
	ouput   *spi.Page

	sourceLayout  []*plan.Symbol
	outputColumns []*spi.Column
	exprs         []expression.Expression
}

func NewProjectionOperator(project *plan.ProjectionNode, sourceLayout []*plan.Symbol) Operator {
	return &ProjectionOperator{project: project, sourceLayout: sourceLayout}
}

// AddInput implements Operator.
func (h *ProjectionOperator) AddInput(page *spi.Page) {
	h.source = page
}

// Finish implements Operator.
func (h *ProjectionOperator) Finish() {
}

// GetOutput implements Operator.
func (h *ProjectionOperator) GetOutput() *spi.Page {
	if len(h.exprs) == 0 {
		h.prepare()
	}
	it := h.source.Iterator()
	for row := it.Begin(); row != it.End(); row = it.Next() {
		for i, expr := range h.exprs {
			fmt.Printf("do ..... projection op expr %T,%s ret type=%v\n", expr, expr.String(), expr.GetType().String())
			switch expr.GetType() {
			case types.DataTypeString:
				val, _, _ := expr.EvalString(row)
				h.outputColumns[i].AppendString(val)
			case types.DataTypeInt:
				val, _, _ := expr.EvalInt(row)
				h.outputColumns[i].AppendInt(val)
			case types.DataTypeFloat:
				val, _, _ := expr.EvalFloat(row)
				h.outputColumns[i].AppendFloat(val)
			case types.DataTypeTimeSeries, types.DataTypeSum, types.DataTypeFirst, types.DataTypeLast, types.DataTypeMin, types.DataTypeMax, types.DataTypeHistogram:
				val, _, _ := expr.EvalTimeSeries(row)
				h.outputColumns[i].AppendTimeSeries(val)
			default:
				panic("unsupport data type:" + expr.GetType().String())
			}
		}
	}
	return h.ouput
}

// IsFinished implements Operator.
func (h *ProjectionOperator) IsFinished() bool {
	return true
}

func (h *ProjectionOperator) prepare() {
	h.exprs = make([]expression.Expression, len(h.project.Assignments))
	h.outputColumns = make([]*spi.Column, len(h.project.Assignments))
	h.ouput = spi.NewPage()
	for i, assign := range h.project.Assignments {
		h.exprs[i] = expression.Rewrite(&expression.RewriteContext{
			SourceLayout: h.sourceLayout,
		}, assign.Expression)
		h.outputColumns[i] = spi.NewColumn()
		h.ouput.AppendColumn(spi.NewColumnInfo(assign.Symbol.Name, assign.Symbol.DataType), h.outputColumns[i])
	}
}
