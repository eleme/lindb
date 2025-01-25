package infoschema

import (
	"context"
	"fmt"

	"github.com/samber/lo"

	"github.com/lindb/lindb/meta"
	"github.com/lindb/lindb/spi"
	"github.com/lindb/lindb/spi/types"
	"github.com/lindb/lindb/sql/tree"
)

type SplitSourceProvider struct {
	metadataMgr meta.MetadataManager
}

func NewSplitSourceProvider(metadataMgr meta.MetadataManager) spi.SplitSourceProvider {
	return &SplitSourceProvider{
		metadataMgr: metadataMgr,
	}
}

func (s *SplitSourceProvider) CreateSplitSources(ctx context.Context, table spi.TableHandle, partitions []int,
	outputColumns []types.ColumnMetadata, assignments []*spi.ColumnAssignment,
	predicate tree.Expression,
) (splits []spi.SplitSource) {
	infoTable, ok := table.(*TableHandle)
	if !ok {
		panic(fmt.Sprintf("information schema provider not support table handle<%T>", table))
	}
	schema, ok := GetTableSchema(infoTable.Table)
	if !ok {
		panic(fmt.Errorf("information table schema not found: %s", infoTable.Table))
	}
	colIdxs := make([]int, len(outputColumns))
	for i, col := range outputColumns {
		if _, idx, exist := lo.FindIndexOf(schema.Columns, func(item types.ColumnMetadata) bool {
			return item.Name == col.Name
		}); exist {
			colIdxs[i] = idx
		}
	}
	if len(colIdxs) != len(outputColumns) {
		return nil
	}
	return []spi.SplitSource{
		&SplitSource{
			split: &InfoSplit{
				table:         infoTable.Table,
				predicate:     predicate,
				outputColumns: outputColumns,
				colIdxs:       colIdxs,
			},
		},
	}
}

type SplitSource struct {
	split     *InfoSplit
	completed bool
}

func (s *SplitSource) Prepare() {
}

func (s *SplitSource) HasNext() bool {
	return !s.completed
}

func (s *SplitSource) Next() spi.Split {
	defer func() {
		s.completed = true
	}()
	return s.split
}
