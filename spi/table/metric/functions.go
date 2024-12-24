package metric

import (
	"fmt"

	"github.com/lindb/lindb/series/field"
	"github.com/lindb/lindb/sql/tree"
)

func getAggFunc(funcName tree.FuncName) field.AggType {
	switch funcName {
	case tree.Sum:
		return field.Sum
	case tree.Max:
		return field.Max
	case tree.Min:
		return field.Min
	case tree.Last:
		return field.Last
	case tree.First:
		return field.First
	default:
		panic(fmt.Sprintf("aggregation function not support: %s", funcName))
	}
}
