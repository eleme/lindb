package fields

import (
	"github.com/lindb/lindb/aggregation/function"
	"github.com/lindb/lindb/pkg/collections"
	"github.com/lindb/lindb/series"
	"github.com/lindb/lindb/series/field"
)

// singleField represents the single field series
type singleField struct {
	value     collections.FloatArray
	fieldType field.Type
}

// NewSingleField creates a single field series
func NewSingleField(capacity int, fieldType field.Type, it series.FieldIterator) Field {
	if it.HasNext() {
		value := collections.NewFloatArray(capacity)
		primitiveIt := it.Next()
		for primitiveIt.HasNext() {
			slot, val := primitiveIt.Next()
			value.SetValue(slot, val)
		}
		return &singleField{value: value, fieldType: fieldType}
	}
	return nil
}

// GetValues returns the values which function call need by given function type and field type
func (f *singleField) GetValues(funcType function.FuncType) []collections.FloatArray {
	switch {
	case funcType == function.Sum && f.fieldType == field.SumField:
		return []collections.FloatArray{f.value}
	case funcType == function.Max && f.fieldType == field.MaxField:
		return []collections.FloatArray{f.value}
	default:
		return nil
	}
}

// GetDefaultValues returns the field default values which aggregation need by field type
func (f *singleField) GetDefaultValues() []collections.FloatArray {
	return []collections.FloatArray{f.value}
}
