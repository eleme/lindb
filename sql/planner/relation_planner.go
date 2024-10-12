package planner

import (
	"fmt"
	"time"

	"github.com/lindb/lindb/pkg/timeutil"
	"github.com/lindb/lindb/spi/types"
	"github.com/lindb/lindb/sql/analyzer"
	"github.com/lindb/lindb/sql/context"
	"github.com/lindb/lindb/sql/expression"
	planpkg "github.com/lindb/lindb/sql/planner/plan"
	"github.com/lindb/lindb/sql/tree"
)

type RelationPlanner struct {
	outerContext   *TranslationMap
	context        *context.PlannerContext
	timePredicates []*tree.TimePredicate
}

func NewRelationPlanner(context *context.PlannerContext, outerContext *TranslationMap,
	timePredicates []*tree.TimePredicate,
) tree.Visitor {
	return &RelationPlanner{
		context:        context,
		outerContext:   outerContext,
		timePredicates: timePredicates,
	}
}

func (p *RelationPlanner) Visit(context any, n tree.Node) (r any) {
	switch node := n.(type) {
	case *tree.Query:
		return p.visitQuery(context, node)
	case *tree.QuerySpecification:
		return p.visitQuerySpecification(context, node)
	case *tree.Join:
		return p.visitJoin(context, node)
	case *tree.AliasedRelation:
		return p.visitAliasedRelation(context, node)
	case *tree.Table:
		return p.visitTable(context, node)
	case *tree.Values:
		return p.visitValues(context, node)
	default:
		panic(fmt.Sprintf("relation analyzer unsupport node:%T", n))
	}
}

func (p *RelationPlanner) visitQuery(context any, node *tree.Query) (r any) {
	return NewQueryPlanner(p.context, p.outerContext).planQuery(node)
}

func (p *RelationPlanner) visitQuerySpecification(context any, node *tree.QuerySpecification) (r any) {
	return NewQueryPlanner(p.context, p.outerContext).planQuerySpecification(node)
}

func (p *RelationPlanner) visitJoin(context any, node *tree.Join) (r any) {
	leftPlan := node.Left.Accept(context, p).(*RelationPlan)
	rightPlan := node.Right.Accept(context, p).(*RelationPlan)
	criteria := node.Criteria
	if _, ok := criteria.(*tree.JoinUsing); ok {
		return p.planJoinUsing(node, leftPlan, rightPlan)
	}
	return p.planJoin(node, p.context.AnalyzerContext.Analysis.GetScope(node), leftPlan, rightPlan)
}

func (p *RelationPlanner) visitAliasedRelation(context any, node *tree.AliasedRelation) (r any) {
	subPlan := node.Relation.Accept(context, p).(*RelationPlan)
	root := subPlan.Root
	mappings := subPlan.FieldMappings
	return &RelationPlan{
		Root:          root,
		OutContext:    p.outerContext,
		Scope:         p.context.AnalyzerContext.Analysis.GetScope(node),
		FieldMappings: mappings,
	}
}

func (p *RelationPlanner) visitValues(_ any, node *tree.Values) (r any) {
	scope := p.context.AnalyzerContext.Analysis.GetScope(node)
	var outputSymbols []*planpkg.Symbol
	for i := range scope.RelationType.Fields {
		symbol := &planpkg.Symbol{
			Name:     scope.RelationType.Fields[i].Name,
			DataType: scope.RelationType.Fields[i].DataType,
		}
		outputSymbols = append(outputSymbols, symbol)
	}
	return &RelationPlan{
		Root: &planpkg.ValuesNode{
			BaseNode: planpkg.BaseNode{
				ID: p.context.PlanNodeIDAllocator.Next(),
			},
			Rows:          node.Rows,
			RowCount:      node.Rows.NumRows(),
			OutputSymbols: outputSymbols,
		},
		OutContext:    p.outerContext,
		Scope:         scope,
		FieldMappings: outputSymbols,
	}
}

func (p *RelationPlanner) visitTable(context any, node *tree.Table) (r any) {
	fmt.Printf("visit table, time predicates=%v\n", p.timePredicates)
	namedQuery := p.context.AnalyzerContext.Analysis.GetNamedQuery(node)
	scope := p.context.AnalyzerContext.Analysis.GetScope(node)
	var plan *RelationPlan
	if namedQuery != nil {
		// process named query ref
		subPlan := namedQuery.Accept(nil, p).(*RelationPlan)
		// FIXME:???
		coerced := coerce(subPlan, nil, nil, nil)
		plan = &RelationPlan{
			Root:          coerced.Node,
			Scope:         scope,
			FieldMappings: coerced.Fields,
		}
	} else {
		var outputSymbols []*planpkg.Symbol
		for i := range scope.RelationType.Fields {
			symbol := &planpkg.Symbol{
				Name:     scope.RelationType.Fields[i].Name, // FIXME: id allocator
				DataType: scope.RelationType.Fields[i].DataType,
			}
			outputSymbols = append(outputSymbols, symbol)
		}

		fmt.Printf("table visit relation plan====%v\n", outputSymbols)
		tableHandle := p.context.AnalyzerContext.Analysis.GetTableHandle(node)
		tableMetadata := p.context.AnalyzerContext.Analysis.GetTableMetadata(tableHandle.String())
		root := planpkg.NewTableScanNode(p.context.PlanNodeIDAllocator.Next())
		root.Table = p.context.AnalyzerContext.Analysis.GetTableHandle(node)
		timeRange := timeutil.TimeRange{
			Start: time.Now().UnixMilli() - time.Hour.Milliseconds(),
			End:   time.Now().UnixMilli(),
		}
		if len(p.timePredicates) > 0 {
			translations := &TranslationMap{context: p.context}
			evalCtx := expression.NewEvalContext(p.context.Context)
			// if has time predicate, add time range filter for table handle
			for _, timePredicate := range p.timePredicates {
				timestamp, _ := expression.EvalTime(evalCtx, translations.Rewrite(timePredicate.Value))
				switch timePredicate.Operator {
				case tree.ComparisonGT:
					timeRange.Start = timestamp.UnixMilli()
				case tree.ComparisonLT:
					timeRange.End = timestamp.UnixMilli()
				}
			}
		}
		root.Table.SetTimeRange(timeRange)

		root.OutputSymbols = outputSymbols
		root.Partitions = tableMetadata.Partitions
		plan = &RelationPlan{
			Root:          root,
			Scope:         scope,
			FieldMappings: outputSymbols,
			OutContext:    p.outerContext,
		}
	}
	return plan
}

func (p *RelationPlanner) planJoinUsing(node *tree.Join, left, right *RelationPlan) *RelationPlan {
	return &RelationPlan{}
}

func (p *RelationPlanner) planJoin(node *tree.Join, scope *analyzer.Scope, left, right *RelationPlan) *RelationPlan {
	var outputSymbols []*planpkg.Symbol
	outputSymbols = append(outputSymbols, left.FieldMappings...)
	outputSymbols = append(outputSymbols, right.FieldMappings...)

	var joinCriteriaClauses []*planpkg.EqualJoinCriteria
	leftPlanBuilder := newPlanBuilder(p.context, left, nil)
	rightPlanBuilder := newPlanBuilder(p.context, right, nil)
	if node.Type != tree.CROSS && node.Type != tree.IMPLICIT {
		criteria := p.context.AnalyzerContext.Analysis.GetJoinCriteria(node)
		expressions := analyzer.ExtractConjuncts(criteria)
		var leftComparisonExpressions []tree.Expression
		var rightComparisonExpressions []tree.Expression
		var joinConditionComparisonOperators []tree.ComparisonOperator
		for i := range expressions {
			conjunct := expressions[i]

			if comparisonExpression, ok := conjunct.(*tree.ComparisonExpression); ok {
				firstExpression := comparisonExpression.Left
				secondExpression := comparisonExpression.Right
				leftComparisonExpressions = append(leftComparisonExpressions, firstExpression)
				rightComparisonExpressions = append(rightComparisonExpressions, secondExpression)
				joinConditionComparisonOperators = append(joinConditionComparisonOperators, comparisonExpression.Operator)
			}
			// TODO: check not equal

			fmt.Println(conjunct)
		}

		// add projections for join criteria
		leftPlanBuilder = leftPlanBuilder.appendProjections(leftComparisonExpressions)
		rightPlanBuilder = rightPlanBuilder.appendProjections(rightComparisonExpressions)

		leftCoercions := coerceExpressions(leftPlanBuilder, leftComparisonExpressions, p.context.SymbolAllocator, p.context.PlanNodeIDAllocator)
		rightCoercions := coerceExpressions(rightPlanBuilder, rightComparisonExpressions, p.context.SymbolAllocator, p.context.PlanNodeIDAllocator)
		fmt.Println(leftCoercions)
		for i := range leftComparisonExpressions {
			if joinConditionComparisonOperators[i] == tree.ComparisonEQ {
				leftSymbol := leftCoercions.mappings[leftComparisonExpressions[i]]
				rightSymbol := rightCoercions.mappings[rightComparisonExpressions[i]]
				joinCriteriaClauses = append(joinCriteriaClauses, &planpkg.EqualJoinCriteria{
					Left:  leftSymbol,
					Right: rightSymbol,
				})
			}
		}
	}

	root := &planpkg.JoinNode{
		BaseNode: planpkg.BaseNode{
			ID: p.context.PlanNodeIDAllocator.Next(),
		},
		Type:     planpkg.JoinTypeConvert(node.Type),
		Left:     leftPlanBuilder.root,
		Right:    rightPlanBuilder.root,
		Criteria: joinCriteriaClauses,
	}
	return &RelationPlan{
		Root:          root,
		FieldMappings: outputSymbols,
		OutContext:    p.outerContext,
	}
}

func coerce(plan *RelationPlan, types []types.Type, symbolAllocator *planpkg.SymbolAllocator, idAllocator *planpkg.PlanNodeIDAllocator) *NodeAndMappings {
	return nil
}

func coerceExpressions(subPlan *PlanBuilder, expressions []tree.Expression, symbolAllocator *planpkg.SymbolAllocator, idAllocator *planpkg.PlanNodeIDAllocator) *PlanAndMappings {
	mappings := make(map[tree.Expression]*planpkg.Symbol)

	for i := range expressions {
		expression := expressions[i]
		if _, ok := mappings[expression]; !ok {
			// TODO: need modify
			symbol := symbolAllocator.NewSymbol(subPlan.translations.Rewrite(expression), "", types.DTFloat) // TODO: get type from context
			mappings[expression] = symbol
		}
	}
	return &PlanAndMappings{
		mappings: mappings,
	}
}
