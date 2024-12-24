package metric

import "github.com/lindb/lindb/internal/concurrent"

type buildGrouping struct {
	concurrent.Task
}

func newBuildGrouping() *concurrent.Task {
	t := &buildGrouping{}
	return concurrent.NewTask(t.handle, t.handleError)
}

func (t *buildGrouping) handle() {}

func (t *buildGrouping) handleError(err error) {}
