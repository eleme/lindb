package execution

import (
	"context"

	"github.com/lindb/lindb/constants"
	"github.com/lindb/lindb/sql/tree"
)

type DropDatabaseTask struct {
	deps      *Deps
	statement *tree.DropDatabase
}

func NewDropDatabaseTask(deps *Deps, statement *tree.DropDatabase) DataDefinitionTask {
	return &DropDatabaseTask{deps: deps, statement: statement}
}

// Name implements DataDefinitionTask.
func (d *DropDatabaseTask) Name() string {
	return "DROP DATABASE"
}

// Execute implements DataDefinitionTask.
func (d *DropDatabaseTask) Execute(ctx context.Context) error {
	if d.statement.Exists {
		if _, exist := d.deps.MetaMgr.GetDatabase(d.statement.Name); !exist {
			return constants.ErrDatabaseNotExist
		}
	}
	return d.deps.MetaMgr.DropDatabase(ctx, d.statement.Name)
}
