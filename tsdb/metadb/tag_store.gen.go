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

// Code generated by tmpl; DO NOT EDIT.
// https://github.com/benbjohnson/tmpl
//
// Source: int_map.tmpl

package metadb

import (
	"github.com/lindb/roaring"
)

// TagStore represents int map using roaring bitmap
type TagStore struct {
	putCount int             // insert count
	keys     *roaring.Bitmap // store all keys
	values   [][]TagEntry    // store all values by high/low key
}

// NewTagStore creates a int map
func NewTagStore() *TagStore {
	return &TagStore{
		keys: roaring.New(),
	}
}

// Get returns value by key, if exist returns it, else returns nil, false
func (m *TagStore) Get(key uint32) (TagEntry, bool) {
	if len(m.values) == 0 {
		return nil, false
	}
	// get high index
	found, highIdx := m.keys.ContainsAndRankForHigh(key)
	if !found {
		return nil, false
	}
	// get low index
	found, lowIdx := m.keys.ContainsAndRankForLow(key, highIdx-1)
	if !found {
		return nil, false
	}
	return m.values[highIdx-1][lowIdx-1], true
}

// Put puts the value by key
func (m *TagStore) Put(key uint32, value TagEntry) {
	defer m.tryOptimize()
	if len(m.values) == 0 {
		// if values is empty, append new low container directly
		m.values = append(m.values, []TagEntry{value})

		m.keys.Add(key)
		return
	}
	found, highIdx := m.keys.ContainsAndRankForHigh(key)
	if !found {
		// high container not exist, insert it
		stores := m.values
		// insert operation, insert high values
		stores = append(stores, nil)
		copy(stores[highIdx+1:], stores[highIdx:len(stores)-1])
		stores[highIdx] = []TagEntry{value}
		m.values = stores

		m.keys.Add(key)
		return
	}
	// high container exist
	lowIdx := m.keys.RankForLow(key, highIdx-1)
	stores := m.values[highIdx-1]
	// insert operation
	stores = append(stores, nil)
	copy(stores[lowIdx+1:], stores[lowIdx:len(stores)-1])
	stores[lowIdx] = value
	m.values[highIdx-1] = stores

	m.keys.Add(key)
}

// tryOptimize optimizes the roaring bitmap when inserted in every 100
func (m *TagStore) tryOptimize() {
	m.putCount++
	if m.putCount%100 == 0 {
		m.keys.RunOptimize()
	}
}

// Keys returns the all keys
func (m *TagStore) Keys() *roaring.Bitmap {
	return m.keys
}

// Values returns the all values
func (m *TagStore) Values() [][]TagEntry {
	return m.values
}

// Size returns the size of keys
func (m *TagStore) Size() int {
	return int(m.keys.GetCardinality())
}

// WalkEntry walks each kv entry via fn.
func (m *TagStore) WalkEntry(fn func(key uint32, value TagEntry) error) error {
	values := m.values
	keys := m.keys
	highKeys := keys.GetHighKeys()
	for highIdx, highKey := range highKeys {
		hk := uint32(highKey) << 16
		lowValues := values[highIdx]
		lowContainer := keys.GetContainerAtIndex(highIdx)
		it := lowContainer.PeekableIterator()
		idx := 0
		for it.HasNext() {
			lowKey := it.Next()
			value := lowValues[idx]
			idx++
			if err := fn(uint32(lowKey&0xFFFF)|hk, value); err != nil {
				return err
			}
		}
	}
	return nil
}
