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

package tagindex

import (
	"github.com/lindb/roaring"

	"github.com/lindb/lindb/kv"
)

var SeriesForwardMerger kv.MergerType = "SeriesForwardMerger"

// init registers series forward merger create function
func init() {
	kv.RegisterMerger(SeriesForwardMerger, NewForwardMerger)
}

// forwardMerger implements kv.Merger for merging forward index data for each tag key
type forwardMerger struct {
	forwardFlusher ForwardFlusher
	kvFlusher      kv.Flusher
}

func (m *forwardMerger) Init(_ map[string]interface{}) {}

// NewForwardMerger creates a forward merger
func NewForwardMerger(flusher kv.Flusher) (kv.Merger, error) {
	forwardFlusher, err := NewForwardFlusher(flusher)
	if err != nil {
		return nil, err
	}
	return &forwardMerger{
		kvFlusher:      flusher,
		forwardFlusher: forwardFlusher,
	}, nil
}

// Merge merges the multi forward index data into a forward index for same tag key id
func (m *forwardMerger) Merge(key uint32, values [][]byte) error {
	var scanners []*tagForwardScanner
	seriesIDs := roaring.New() // target merged series ids
	// 1. prepare tag forward scanner
	for _, value := range values {
		reader, err := NewTagForwardReader(value)
		if err != nil {
			return err
		}
		seriesIDs.Or(reader.GetSeriesIDs())
		scanners = append(scanners, newTagForwardScanner(reader))
	}

	// 2. merge forward index by roaring container
	highKeys := seriesIDs.GetHighKeys()
	m.forwardFlusher.PrepareTagKey(key)

	for idx, highKey := range highKeys {
		container := seriesIDs.GetContainerAtIndex(idx)
		it := container.PeekableIterator()
		var tagValueIDs []uint32
		for it.HasNext() {
			lowSeriesID := it.Next()
			// scan index data then merge tag value ids, sort by series id
			for _, scanner := range scanners {
				tagValueIDs = scanner.scan(highKey, lowSeriesID, tagValueIDs)
			}
		}
		// flush tag value ids by one container
		if err := m.forwardFlusher.FlushForwardIndex(tagValueIDs); err != nil {
			return err
		}
	}
	// flush all series ids under this tag key
	return m.forwardFlusher.CommitTagKey(seriesIDs)
}
