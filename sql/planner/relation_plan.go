package planner

import (
	"fmt"

	"github.com/lindb/lindb/sql/analyzer"
	"github.com/lindb/lindb/sql/planner/plan"
	"github.com/lindb/lindb/sql/tree"
)

type RelationPlan struct {
	Root          plan.PlanNode
	Scope         *analyzer.Scope
	OutContext    *TranslationMap
	FieldMappings []*plan.Symbol
}

func (r *RelationPlan) getSymbol(fieldIndex tree.FieldIndex) *plan.Symbol {
	fieldIdx := int(fieldIndex)
	if fieldIdx < 0 || fieldIdx >= len(r.FieldMappings) {
		panic(fmt.Sprintf("no field->symbol mapping for field %d", fieldIdx))
	}
	fmt.Printf("get symbol %v\n", r.FieldMappings)
	return r.FieldMappings[fieldIdx]
}
