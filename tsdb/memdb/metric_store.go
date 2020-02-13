package memdb

import (
	"sort"

	"github.com/lindb/roaring"

	"github.com/lindb/lindb/flow"
	"github.com/lindb/lindb/series/field"
	"github.com/lindb/lindb/tsdb/tblstore/metricsdata"
)

//go:generate mockgen -source ./metric_store.go -destination=./metric_store_mock.go -package memdb

// for testing
var (
	flushFunc = flush
)

const emptyMStoreSize = 8 + // mutable
	8 + // atomic.Value
	4 + // uint32
	4 + // uint32
	4 // int32

// mStoreINTF abstracts a metricStore
type mStoreINTF interface {
	// flow.DataFilter filters the data based on condition
	flow.DataFilter
	// SetTimestamp sets the current write timestamp
	SetTimestamp(familyID uint8, slot uint16)
	// AddField adds field meta into metric level
	AddField(fieldID uint16, fieldType field.Type)
	// GetOrCreateTStore constructs the index and return a tStore
	GetOrCreateTStore(seriesID uint32) (tStore tStoreINTF, createdSize int)
	// FlushMetricsDataTo flushes metric-block of mStore to the Writer.
	FlushMetricsDataTo(tableFlusher metricsdata.Flusher, flushCtx flushContext) (err error)
}

// metricStore represents metric level storage, stores all series data, and fields/family times metadata
type metricStore struct {
	MetricStore

	families map[uint8]*familySlotRange // time slot range
	fields   field.Metas                // field metadata
}

// newMetricStore returns a new mStoreINTF.
func newMetricStore() mStoreINTF {
	ms := metricStore{
		families: make(map[uint8]*familySlotRange),
	}
	ms.keys = roaring.New() // init keys
	return &ms
}

// SetTimestamp sets the current write timestamp
func (ms *metricStore) SetTimestamp(familyID uint8, slot uint16) {
	slotRange, ok := ms.families[familyID]
	if !ok {
		ms.families[familyID] = newFamilySlotRange(slot, slot)
	} else {
		slotRange.setSlot(slot)
	}
}

// AddField adds field meta into metric level
func (ms *metricStore) AddField(fieldID uint16, fieldType field.Type) {
	_, ok := ms.fields.GetFromID(fieldID)
	if !ok {
		ms.fields = ms.fields.Insert(field.Meta{
			ID:   fieldID,
			Type: fieldType,
		})
		// sort by field id
		sort.Slice(ms.fields, func(i, j int) bool { return ms.fields[i].ID < ms.fields[j].ID })
	}
}

// GetOrCreateTStore constructs the index and return a tStore
func (ms *metricStore) GetOrCreateTStore(seriesID uint32) (tStore tStoreINTF, createdSize int) {
	tStore, ok := ms.Get(seriesID)
	if !ok {
		tStore = newTimeSeriesStore()
		ms.Put(seriesID, tStore)
		createdSize += emptyTimeSeriesStoreSize
	}
	return tStore, createdSize
}

// FlushMetricsTo Writes metric-data to the table.
func (ms *metricStore) FlushMetricsDataTo(flusher metricsdata.Flusher, flushCtx flushContext) (err error) {
	// family time not exist, return
	slotRange, ok := ms.families[flushCtx.familyID]
	if !ok {
		return
	}
	// field not exist, return
	fieldLen := len(ms.fields)
	if fieldLen == 0 {
		return
	}
	// flush field meta info
	flusher.FlushFieldMetas(ms.fields)
	// set current family's slot range
	flushCtx.start, flushCtx.end = slotRange.getSlotRange()
	if err := ms.WalkEntry(func(key uint32, value tStoreINTF) error {
		return flushFunc(flusher, flushCtx, key, value)
	}); err != nil {
		return err
	}
	return flusher.FlushMetric(flushCtx.metricID)
}

// flush flushes series data
func flush(flusher metricsdata.Flusher, flushCtx flushContext, key uint32, value tStoreINTF) error {
	value.FlushSeriesTo(flusher, flushCtx)
	flusher.FlushSeries(key)
	return nil
}
