package spi

import (
	"context"

	"github.com/lindb/lindb/spi/types"
	"github.com/lindb/lindb/sql/tree"
)

// PageSource represents a page of data source.
type PageSource interface {
	AddSplit(split Split)
	GetNextPage() *types.Page
}

type SplitSource interface {
	Prepare()
	HasNext() bool
	Next() Split
}

type SplitSourceProvider interface {
	CreateSplitSources(ctx context.Context, table TableHandle, partitions []int,
		outputColumns []types.ColumnMetadata, assignments []*ColumnAssignment,
		predicate tree.Expression) (splits []SplitSource)
}

type PageSourceConnector interface {
	GetPages() <-chan *types.Page
}

type PageSourceConnectorProvider interface {
	CreatePageSourceConnector(ctx context.Context,
		table TableHandle, partitions []int, // table info
		predicate tree.Expression, // predicate
		outputColumns []types.ColumnMetadata, assignments []*ColumnAssignment, // output
	) PageSourceConnector
}

type PageSourceProvider interface {
	CreatePageSource(ctx context.Context) PageSource
}

type PageSourceManager struct{}

type SplitSourceFactory struct{}
