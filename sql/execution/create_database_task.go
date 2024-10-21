package execution

import (
	"context"

	"github.com/lindb/common/pkg/encoding"

	"github.com/lindb/lindb/models"
	"github.com/lindb/lindb/pkg/option"
	"github.com/lindb/lindb/sql/expression"
	"github.com/lindb/lindb/sql/tree"
)

type CreateDatabaseTask struct {
	deps      *Deps
	statement *tree.CreateDatabase
}

func NewCreateDatabaseTask(deps *Deps, statement *tree.CreateDatabase) DataDefinitionTask {
	return &CreateDatabaseTask{
		deps:      deps,
		statement: statement,
	}
}

func (task *CreateDatabaseTask) Name() string {
	return "CREATE DATABASE"
}

func (task *CreateDatabaseTask) Execute(ctx context.Context) error {
	// FIXME: check database exist
	engineType := models.Metric
	for _, option := range task.statement.CreateOptions {
		switch createOption := option.(type) {
		case *tree.EngineOption:
			engineType = createOption.Type
		default:
			panic("unknown option type")
		}
	}
	evalCtx := expression.NewEvalContext(ctx)
	switch engineType {
	case models.Metric:
		// FIXME: need check alive node/shard/replica
		database, err := task.buildMetricDatabase(evalCtx, engineType)
		if err != nil {
			return err
		}
		// save database config
		// TODO: remove metadata manager
		if err := task.deps.MetaMgr.CreateDatabase(ctx, database); err != nil {
			return err
		}
	}

	return nil
}

func (task *CreateDatabaseTask) buildMetricDatabase(
	evalCtx expression.EvalContext,
	engineType models.EngineType,
) (*models.Database, error) {
	options := option.DatabaseOption{}
	if err := task.evalPropsExpression(evalCtx, task.statement.Props, &options); err != nil {
		return nil, err
	}
	// rollup interval options
	for _, rollup := range task.statement.Rollup {
		rollupOption := option.Interval{}
		if err := task.evalPropsExpression(evalCtx, rollup.Props, &rollupOption); err != nil {
			return nil, err
		}
		options.Intervals = append(options.Intervals, rollupOption)
	}
	database := &models.Database{
		Name:   task.statement.Name,
		Engine: engineType,
		Option: &options,
	}
	database.Default()
	if err := database.Validate(); err != nil {
		return nil, err
	}
	return database, nil
}

func (task *CreateDatabaseTask) evalPropsExpression(evalCtx expression.EvalContext, props []*tree.Property, result any) error {
	values := make(map[string]any)
	for _, prop := range props {
		val, err := expression.Eval(evalCtx, prop.Value)
		if err != nil {
			return err
		}
		values[prop.Name.Value] = val
	}
	data := encoding.JSONMarshal(values)
	if err := encoding.JSONUnmarshal(data, result); err != nil {
		return err
	}
	return nil
}
