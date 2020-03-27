package memdb

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/lindb/roaring"
	"github.com/stretchr/testify/assert"

	"github.com/lindb/lindb/aggregation"
	"github.com/lindb/lindb/constants"
	"github.com/lindb/lindb/flow"
	"github.com/lindb/lindb/series/field"
)

func TestMetricStore_Filter(t *testing.T) {
	metricStore := mockMetricStore()

	// case 1: field not found
	rs, err := metricStore.Filter([]field.ID{1, 2}, nil, nil)
	assert.Equal(t, constants.ErrNotFound, err)
	assert.Nil(t, rs)
	// case 2: family not found
	rs, err = metricStore.Filter([]field.ID{1, 20}, nil, map[familyID]int64{
		familyID(10): 100,
	})
	assert.Equal(t, constants.ErrNotFound, err)
	assert.Nil(t, rs)
	// case 3: series ids not found
	rs, err = metricStore.Filter([]field.ID{1, 20}, roaring.BitmapOf(1, 2), map[familyID]int64{
		familyID(20): 100,
	})
	assert.Equal(t, constants.ErrNotFound, err)
	assert.Nil(t, rs)
	// case 3: found data
	rs, err = metricStore.Filter([]field.ID{1, 20}, roaring.BitmapOf(1, 100, 200), map[familyID]int64{
		familyID(20): 100,
	})
	assert.NoError(t, err)
	assert.NotNil(t, rs)
	mrs := rs[0].(*memFilterResultSet)
	assert.EqualValues(t, roaring.BitmapOf(100, 200).ToArray(), mrs.SeriesIDs().ToArray())
	assert.Equal(t, []familyID{20}, mrs.familyIDs)
	assert.Equal(t,
		map[familyID]int64{
			familyID(20): 100,
		}, mrs.familyIDMap)
	assert.Equal(t,
		field.Metas{{
			ID:   20,
			Type: field.SumField,
		}}, mrs.fields)
	assert.Equal(t, "memory", rs[0].Identifier())
}

func TestMemFilterResultSet_Load(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	qFlow := flow.NewMockStorageQueryFlow(ctrl)
	mStore := mockMetricStore()

	rs, err := mStore.Filter([]field.ID{1, 20}, roaring.BitmapOf(1, 100, 200), map[familyID]int64{
		familyID(1):  100,
		familyID(20): 1000,
	})
	assert.NoError(t, err)
	sAgg := aggregation.NewMockSeriesAggregator(ctrl)
	fAgg := aggregation.NewMockFieldAggregator(ctrl)
	pAgg := aggregation.NewMockPrimitiveAggregator(ctrl)
	// case 1: load data success
	gomock.InOrder(
		qFlow.EXPECT().GetAggregator().Return(aggregation.FieldAggregates{sAgg}),
		sAgg.EXPECT().GetAggregator(int64(100)).Return(fAgg, false),
		sAgg.EXPECT().GetAggregator(int64(1000)).Return(fAgg, true),
		fAgg.EXPECT().GetAllAggregators().Return([]aggregation.PrimitiveAggregator{pAgg}),
		pAgg.EXPECT().FieldID().Return(field.PrimitiveID(10)),
		qFlow.EXPECT().Reduce("host", gomock.Any()),
	)
	rs[0].Load(qFlow, []field.ID{20, 30}, 0, map[string][]uint16{
		"host": {1, 2},
	})
	// case 2: series ids not found
	gomock.InOrder(
		qFlow.EXPECT().GetAggregator().Return(aggregation.FieldAggregates{sAgg}),
		sAgg.EXPECT().GetAggregator(int64(100)).Return(fAgg, false),
		sAgg.EXPECT().GetAggregator(int64(1000)).Return(fAgg, true),
		fAgg.EXPECT().GetAllAggregators().Return([]aggregation.PrimitiveAggregator{pAgg}),
		pAgg.EXPECT().FieldID().Return(field.PrimitiveID(10)),
		qFlow.EXPECT().Reduce("host", gomock.Any()),
	)
	rs[0].Load(qFlow, []field.ID{20, 30}, 0, map[string][]uint16{
		"host": {100, 200},
	})
	// case 3: high key not exist
	rs[0].Load(qFlow, []field.ID{20, 30}, 10, map[string][]uint16{
		"host": {100, 200},
	})
	// case 4: field agg is empty
	gomock.InOrder(
		qFlow.EXPECT().GetAggregator().Return(aggregation.FieldAggregates{sAgg}),
		sAgg.EXPECT().GetAggregator(int64(100)).Return(nil, false),
		sAgg.EXPECT().GetAggregator(int64(1000)).Return(nil, false),
		qFlow.EXPECT().Reduce("host", gomock.Any()),
	)
	rs[0].Load(qFlow, []field.ID{20, 30}, 0, map[string][]uint16{
		"host": {100, 200},
	})
}

func mockMetricStore() *metricStore {
	mStore := newMetricStore()
	mStore.AddField(field.ID(10), field.SumField)
	mStore.AddField(field.ID(20), field.SumField)
	mStore.SetTimestamp(familyID(1), 10)
	mStore.SetTimestamp(familyID(20), 20)
	mStore.GetOrCreateTStore(100)
	mStore.GetOrCreateTStore(120)
	mStore.GetOrCreateTStore(200)
	return mStore.(*metricStore)
}
