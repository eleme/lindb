package metricsdata

import (
	"github.com/lindb/lindb/pkg/encoding"
	"github.com/lindb/lindb/series/field"
)

//go:generate mockgen -source ./field_reader.go -destination=./field_reader_mock.go -package metricsdata

// FieldReader represents the field reader when does metric data merge.
// !!!!NOTICE: need get field value in order by field
type FieldReader interface {
	// slotRange returns the time slot range of metric level
	slotRange() (start, end uint16)
	// getFieldData returns the field data by field id,
	// if reader is completed, return nil, if found data returns field data else returns nil
	getFieldData(fieldID field.ID) []byte
	// reset resets the field data for reading
	reset(buf []byte, position int, start, end uint16)
	// close closes the reader
	close()
}

// fieldReader implements FieldReader
type fieldReader struct {
	start, end   uint16
	seriesData   []byte
	fieldOffsets *encoding.FixedOffsetDecoder
	fieldIndexes map[field.ID]int
	fieldCount   int

	completed bool // !!!!NOTICE: need reset completed
}

// newFieldReader creates the field reader
func newFieldReader(fieldIndexes map[field.ID]int, buf []byte, position int, start, end uint16) FieldReader {
	r := &fieldReader{
		fieldIndexes: fieldIndexes,
		fieldCount:   len(fieldIndexes),
	}
	r.reset(buf, position, start, end)
	return r
}

// reset resets the field data for reading
func (r *fieldReader) reset(buf []byte, position int, start, end uint16) {
	r.completed = false
	r.start = start
	r.end = end
	if r.fieldCount == 1 {
		r.seriesData = buf
		return
	}
	data := buf[position:]
	r.fieldOffsets = encoding.NewFixedOffsetDecoder(data)
	r.seriesData = data[r.fieldOffsets.Header()+r.fieldCount*r.fieldOffsets.ValueWidth():]
	r.fieldOffsets = encoding.NewFixedOffsetDecoder(buf[position:])
}

// slotRange returns the time slot range of metric level
func (r *fieldReader) slotRange() (start, end uint16) {
	return r.start, r.end
}

// getFieldData returns the field data by field id,
// if reader is completed, return nil, if found data returns field data else returns nil
func (r *fieldReader) getFieldData(fieldID field.ID) []byte {
	if r.completed {
		return nil
	}
	idx, ok := r.fieldIndexes[fieldID]
	if !ok {
		return nil
	}
	if r.fieldCount == 1 {
		return r.seriesData
	}
	offset, ok := r.fieldOffsets.Get(idx)
	if !ok {
		return nil
	}
	return r.seriesData[offset:]
}

// close marks the reader completed
func (r *fieldReader) close() {
	r.completed = true
}
