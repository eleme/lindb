package version

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/lindb/lindb/kv/table"
)

func TestSnapshot_FindReaders(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fv := NewMockFamilyVersion(ctrl)
	vs := NewMockStoreVersionSet(ctrl)
	fv.EXPECT().GetVersionSet().Return(vs).AnyTimes()
	vs.EXPECT().numberOfLevels().Return(2).AnyTimes()
	v := NewMockVersion(ctrl)
	v.EXPECT().Retain().AnyTimes()
	cache := table.NewMockCache(ctrl)
	snapshot := newSnapshot("test", v, cache)

	// case 1: get reader err
	cache.EXPECT().GetReader("test", Table(table.FileNumber(10))).Return(nil, fmt.Errorf("err"))
	_, err := snapshot.GetReader(table.FileNumber(10))
	assert.Error(t, err)
	// case 2: get reader ok
	cache.EXPECT().GetReader("test", Table(table.FileNumber(11))).Return(table.NewMockReader(ctrl), nil)
	reader, err := snapshot.GetReader(table.FileNumber(11))
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	// case 3: get version
	assert.NotNil(t, snapshot.GetCurrent())
	// case 4: get reader by key
	v.EXPECT().FindFiles(uint32(80)).Return([]*FileMeta{{fileNumber: 10}}).AnyTimes()
	cache.EXPECT().GetReader("test", Table(table.FileNumber(10))).Return(table.NewMockReader(ctrl), nil)
	readers, err := snapshot.FindReaders(uint32(80))
	assert.NoError(t, err)
	assert.Len(t, readers, 1)
	// case 5: cannot get reader by key
	cache.EXPECT().GetReader("test", Table(table.FileNumber(10))).Return(nil, nil)
	readers, err = snapshot.FindReaders(uint32(80))
	assert.NoError(t, err)
	assert.Empty(t, readers)
	// case 6: get reader by key err
	cache.EXPECT().GetReader("test", Table(table.FileNumber(10))).Return(nil, fmt.Errorf("err"))
	readers, err = snapshot.FindReaders(uint32(80))
	assert.Error(t, err)
	assert.Nil(t, readers)
	// case 7: close snapshot
	v.EXPECT().Release()
	snapshot.Close()
	snapshot.Close() // test version release only once
}
