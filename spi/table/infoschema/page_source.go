package infoschema

import (
	"context"
	"fmt"
	"time"

	"github.com/lindb/lindb/spi"
	"github.com/lindb/lindb/spi/types"
)

type PageSourceProvider struct {
	reader Reader
}

func NewPageSourceProvider(reader Reader) spi.PageSourceProvider {
	return &PageSourceProvider{
		reader: reader,
	}
}

// CreatePageSource implements spi.PageSourceProvider.
func (p *PageSourceProvider) CreatePageSource(ctx context.Context) spi.PageSource {
	return &PageSource{
		ctx:    ctx,
		reader: p.reader,
	}
}

type PageSource struct {
	ctx    context.Context
	reader Reader
	split  *InfoSplit
}

// AddSplit implements spi.PageSource.
func (p *PageSource) AddSplit(split spi.Split) {
	if info, ok := split.(*InfoSplit); ok {
		p.split = info
	}
}

// GetNextPage implements spi.PageSource.
func (p *PageSource) GetNextPage() *types.Page {
	rows, err := p.reader.ReadData(p.ctx, p.split.table, p.split.predicate)
	if err != nil {
		panic(err)
	}
	fmt.Printf("info schema: rows=%v\n", rows)
	page := types.NewPage()
	var columns []*types.Column
	outputs := make(map[string]int)
	for idx, output := range p.split.outputColumns {
		column := types.NewColumn()
		page.AppendColumn(output, column)
		columns = append(columns, column)
		outputs[output.Name] = idx
	}
	for _, row := range rows {
		for idx, col := range columns {
			switch p.split.outputColumns[idx].DataType {
			case types.DTString:
				col.AppendString(row[p.split.colIdxs[idx]].String())
			case types.DTFloat:
				col.AppendFloat(row[p.split.colIdxs[idx]].Float())
			case types.DTInt:
				col.AppendInt(row[p.split.colIdxs[idx]].Int())
			case types.DTTimestamp:
				col.AppendTimestamp(time.UnixMilli(row[p.split.colIdxs[idx]].Int()))
			case types.DTDuration:
				col.AppendDuration(row[p.split.colIdxs[idx]].Duration())
			}
		}
	}
	return page
}
