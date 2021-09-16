// Licensed to LinDB under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. LinDB licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package indexdb

import (
	"github.com/lindb/lindb/config"

	"go.uber.org/atomic"
)

// MetricIDMapping represents the metric id mapping,
// tag hash code => series id
type MetricIDMapping interface {
	// GetMetricID return the metric id
	GetMetricID() uint32
	// GetSeriesID gets series id by tags hash, if exist return true
	GetSeriesID(tagsHash uint64) (seriesID uint32, ok bool)
	// GenSeriesID generates series id by tags hash, then cache new series id
	GenSeriesID(tagsHash uint64) (seriesID uint32)
	// RemoveSeriesID removes series id by tags hash
	RemoveSeriesID(tagsHash uint64)
	// AddSeriesID adds the series id init cache
	AddSeriesID(tagsHash uint64, seriesID uint32)
	// SetMaxSeriesIDsLimit sets the max series ids limit
	SetMaxSeriesIDsLimit(limit uint32)
	// GetMaxSeriesIDsLimit returns the max series ids limit
	GetMaxSeriesIDsLimit() uint32
}

// metricIDMapping implements MetricIDMapping interface
type metricIDMapping struct {
	metricID uint32
	// forwardIndex for storing a mapping from tag-hash to the seriesID,
	// purpose of this index is used for fast writing
	hash2SeriesID     map[uint64]uint32
	idSequence        atomic.Uint32
	maxSeriesIDsLimit atomic.Uint32 // maximum number of combinations of series ids
}

// newMetricIDMapping returns a new metric id mapping
func newMetricIDMapping(metricID, sequence uint32) MetricIDMapping {
	return &metricIDMapping{
		metricID:          metricID,
		hash2SeriesID:     make(map[uint64]uint32),
		idSequence:        *atomic.NewUint32(sequence), // first value is 1
		maxSeriesIDsLimit: *atomic.NewUint32(uint32(config.GlobalStorageConfig().TSDB.MaxSeriesIDsNumber)),
	}
}

// GetMetricID return the metric id
func (mim *metricIDMapping) GetMetricID() uint32 {
	return mim.metricID
}

// GetSeriesID gets series id by tags hash, if exist return true
func (mim *metricIDMapping) GetSeriesID(tagsHash uint64) (seriesID uint32, ok bool) {
	seriesID, ok = mim.hash2SeriesID[tagsHash]
	return
}

// AddSeriesID adds the series id init cache
func (mim *metricIDMapping) AddSeriesID(tagsHash uint64, seriesID uint32) {
	mim.hash2SeriesID[tagsHash] = seriesID
}

// GenSeriesID generates series id by tags hash, then cache new series id
func (mim *metricIDMapping) GenSeriesID(tagsHash uint64) (seriesID uint32) {
	// generate new series id
	if mim.maxSeriesIDsLimit.Load() == mim.idSequence.Load() {
		//FIXME too many series id, use max limit????
		seriesID = mim.maxSeriesIDsLimit.Load()
	} else {
		seriesID = mim.idSequence.Inc()
	}
	// cache it
	mim.hash2SeriesID[tagsHash] = seriesID
	return seriesID
}

// RemoveSeriesID removes series id by tags hash
func (mim *metricIDMapping) RemoveSeriesID(tagsHash uint64) {
	seriesID, ok := mim.hash2SeriesID[tagsHash]
	if ok {
		if seriesID == mim.idSequence.Load() {
			mim.idSequence.Dec() // recycle series id
		}
		delete(mim.hash2SeriesID, tagsHash)
	}
}

// SetMaxSeriesIDsLimit sets the max series ids limit
func (mim *metricIDMapping) SetMaxSeriesIDsLimit(limit uint32) {
	mim.maxSeriesIDsLimit.Store(limit)
}

// GetMaxSeriesIDsLimit return the max series ids limit without race condition.
func (mim *metricIDMapping) GetMaxSeriesIDsLimit() uint32 {
	return mim.maxSeriesIDsLimit.Load()
}
