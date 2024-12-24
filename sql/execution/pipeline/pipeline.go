package pipeline

import (
	"fmt"
	"reflect"

	"github.com/lindb/lindb/spi"
	"github.com/lindb/lindb/sql/context"
)

type Pipeline struct {
	taskCtx   *context.TaskContext
	driverFct *DriverFactory

	connector spi.PageSourceConnector
}

func NewPipeline(taskCtx *context.TaskContext, driverFct *DriverFactory) *Pipeline {
	return &Pipeline{
		taskCtx:   taskCtx,
		driverFct: driverFct,
	}
}

func (p *Pipeline) Run() {
	// TODO:remove it?
	// source from exchange(local/remote)
	driver := p.driverFct.CreateDriver()
	sourceOperator := driver.GetSourceOperator()
	fmt.Printf("run driver====%v,%d,%s\n", sourceOperator, sourceOperator.GetSourceID(), reflect.TypeOf(sourceOperator))
	if sourceOperator != nil {
		fmt.Println(p.taskCtx.TaskID)
		// if driver has source operator, register it
		DriverManager.RegisterSourceOperator(p.taskCtx.TaskID, sourceOperator)
	}

	driver.Process()
}
