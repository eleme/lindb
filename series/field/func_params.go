package field

type PrimitiveFields []PrimitiveField

// PrimitiveField represents the function param extract field meta
type PrimitiveField struct {
	FieldID uint16
	AggType AggType
}
