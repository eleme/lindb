package flow

import (
	"testing"

	"github.com/lindb/roaring"
	"github.com/stretchr/testify/assert"
)

func TestLowSeriesIDS_Find(t *testing.T) {
	ids := roaring.BitmapOf(1, 20, 400)
	seriesIDs := NewLowSeriesIDs(ids.GetContainerAtIndex(0))
	idx, ok := seriesIDs.Find(1)
	assert.Equal(t, 0, idx)
	assert.True(t, ok)
	idx, ok = seriesIDs.Find(10)
	assert.Equal(t, -1, idx)
	assert.False(t, ok)
	idx, ok = seriesIDs.Find(20)
	assert.Equal(t, 1, idx)
	assert.True(t, ok)
}
