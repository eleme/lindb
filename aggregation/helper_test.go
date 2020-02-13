package aggregation

import (
	"sort"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/lindb/lindb/pkg/collections"
	"github.com/lindb/lindb/series"
	"github.com/lindb/lindb/series/field"
)

///////////////////////////////////////////////////
//                mock interface				 //
///////////////////////////////////////////////////

// MockSumFieldIterator returns mock an iterator of sum field
func MockSumFieldIterator(ctrl *gomock.Controller, fieldID field.PrimitiveID, points map[int]interface{}) *series.MockFieldIterator {
	it := series.NewMockFieldIterator(ctrl)
	it.EXPECT().HasNext().Return(true)

	primitiveIt := series.NewMockPrimitiveIterator(ctrl)
	it.EXPECT().Next().Return(primitiveIt)

	primitiveIt.EXPECT().FieldID().Return(fieldID)
	primitiveIt.EXPECT().AggType().Return(field.Sum)

	var keys []int
	for timeSlot := range points {
		keys = append(keys, timeSlot)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	for _, timeSlot := range keys {
		primitiveIt.EXPECT().HasNext().Return(true)
		primitiveIt.EXPECT().Next().Return(timeSlot, points[timeSlot])
	}
	// mock nil primitive iterator
	it.EXPECT().HasNext().Return(true)
	it.EXPECT().Next().Return(nil)

	// return hasNext=>false, finish primitive iterator
	primitiveIt.EXPECT().HasNext().Return(false).AnyTimes()

	// sum field only has one primitive field
	it.EXPECT().HasNext().Return(false).AnyTimes()
	return it
}

func AssertPrimitiveIt(t *testing.T, it series.PrimitiveIterator, expect map[int]float64) {
	count := 0
	for it.HasNext() {
		timeSlot, value := it.Next()
		assert.Equal(t, expect[timeSlot], value)
		count++
	}
	assert.Equal(t, count, len(expect))
}

func generateFloatArray(values []float64) collections.FloatArray {
	floatArray := collections.NewFloatArray(len(values))
	for idx, value := range values {
		floatArray.SetValue(idx, value)
	}
	return floatArray
}

// mockSingleIterator returns mock an iterator of single field
func mockSingleIterator(ctrl *gomock.Controller) series.FieldIterator {
	it := series.NewMockFieldIterator(ctrl)
	primitiveIt := series.NewMockPrimitiveIterator(ctrl)
	it.EXPECT().HasNext().Return(true)
	it.EXPECT().Next().Return(primitiveIt)
	primitiveIt.EXPECT().FieldID().Return(field.PrimitiveID(1))
	primitiveIt.EXPECT().HasNext().Return(true)
	primitiveIt.EXPECT().Next().Return(4, 4.0)
	primitiveIt.EXPECT().HasNext().Return(true)
	primitiveIt.EXPECT().Next().Return(50, 50.0)
	primitiveIt.EXPECT().HasNext().Return(false)
	it.EXPECT().HasNext().Return(false)
	return it
}

func mockTimeSeries(ctrl *gomock.Controller, startTime int64, fieldName string, fieldType field.Type) series.Iterator {
	timeSeries := series.NewMockIterator(ctrl)
	timeSeries.EXPECT().FieldType().Return(fieldType)
	timeSeries.EXPECT().FieldName().Return(fieldName)
	it := mockSingleIterator(ctrl)
	timeSeries.EXPECT().HasNext().Return(true)
	timeSeries.EXPECT().Next().Return(startTime, it)
	timeSeries.EXPECT().HasNext().Return(false)
	return timeSeries
}
