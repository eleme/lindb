package expression

import (
	"time"

	"github.com/lindb/lindb/spi/types"
	"github.com/lindb/lindb/sql/tree"
)

func EvalTime(ctx EvalContext, expression tree.Expression) (time.Time, error) {
	expr := Rewrite(&RewriteContext{}, expression)
	val, _, err := expr.EvalTime(ctx, types.EmptyRow)
	return val, err
}

func EvalString(ctx EvalContext, expression tree.Expression) (string, error) {
	expr := Rewrite(&RewriteContext{}, expression)
	val, _, err := expr.EvalString(ctx, types.EmptyRow)
	return val, err
}

func Eval(ctx EvalContext, expression tree.Expression) (val any, err error) {
	expr := Rewrite(&RewriteContext{}, expression)
	switch expr.GetType() {
	case types.DTInt:
		val, _, err = expr.EvalInt(ctx, types.EmptyRow)
	case types.DTFloat:
		val, _, err = expr.EvalFloat(ctx, types.EmptyRow)
	case types.DTString:
		val, _, err = expr.EvalString(ctx, types.EmptyRow)
	case types.DTTimestamp:
		val, _, err = expr.EvalTime(ctx, types.EmptyRow)
	case types.DTDuration:
		val, _, err = expr.EvalDuration(ctx, types.EmptyRow)
	}
	return
}
