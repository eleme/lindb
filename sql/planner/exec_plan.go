package planner

import (
	"context"

	"go.uber.org/atomic"

	"github.com/lindb/lindb/sql/execution/pipeline"
)

type TaskExecutionPlanContext struct {
	ctx             context.Context
	driverFactories []*pipeline.DriverFactory

	nextPipelineID atomic.Int32
}

func NewTaskExecutionPlanContext(ctx context.Context, driverFactories []*pipeline.DriverFactory) *TaskExecutionPlanContext {
	return &TaskExecutionPlanContext{
		ctx:             ctx,
		driverFactories: driverFactories,
	}
}

func (ctx *TaskExecutionPlanContext) AddDriverFactory(physicalOperation *PhysicalOperation) {
	// FIXME: add lookup outer driver?
	driverFct := pipeline.NewDriverFactory(ctx.ctx, ctx.nextPipelineID.Inc(), physicalOperation.operatorFactories)
	ctx.driverFactories = append(ctx.driverFactories, driverFct)
}

type TaskExecutionPlan struct {
	pipelines []*pipeline.Pipeline
}

func NewTaskExecutionPlan(pipelines []*pipeline.Pipeline) *TaskExecutionPlan {
	return &TaskExecutionPlan{
		pipelines: pipelines,
	}
}

func (p *TaskExecutionPlan) GetPipelines() []*pipeline.Pipeline {
	return p.pipelines
}
