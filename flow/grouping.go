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

package flow

import (
	"github.com/lindb/roaring"

	"github.com/lindb/lindb/series/tag"
)

//go:generate mockgen -source=./grouping.go -destination=./grouping_mock.go -package=flow

// GroupingContext represents the context of group by query for tag keys
type GroupingContext interface {
	// BuildGroup builds the grouped series ids by the high key of series id
	// and the container includes low keys of series id
	BuildGroup(highKey uint16, lowSeriesIDs roaring.Container) (scanners []*TagScanner)
	// ScanTagValueIDs scans grouping context by high key/container of series ids,
	// then returns grouped tag value ids for each tag key
	ScanTagValueIDs(highKey uint16, lowSeriesIDs roaring.Container) []*roaring.Bitmap
}

// GroupingScanner represents the scanner which scans the group by data by high key of series id
type GroupingScanner interface {
	// GetSeriesAndTagValue returns group by container and tag value ids
	GetSeriesAndTagValue(highKey uint16) (roaring.Container, []uint32)
	// GetSeriesIDs returns the series ids in current scanner.
	GetSeriesIDs() *roaring.Bitmap
}

// Grouping represents the getter grouping scanners for tag key group by query
type Grouping interface {
	// GetGroupingScanner returns the grouping scanners based on tag key ids and series ids
	GetGroupingScanner(tagKeyID tag.KeyID, seriesIDs *roaring.Bitmap) ([]GroupingScanner, error)
}

// GroupingBuilder represents grouping tag builder.
type GroupingBuilder interface {
	// GetGroupingContext returns the context of group by
	GetGroupingContext(
		groupingTags tag.Metas, seriesIDs *roaring.Bitmap,
	) (*roaring.Bitmap, GroupingContext, error)
}

// groupingContext represents the context of group by query for tag keys
// builds tags => series ids mapping, using such as counting sort
// https://en.wikipedia.org/wiki/Counting_sort
type groupingContext struct {
	tags     tag.Metas // grouping tags
	scanners map[tag.KeyID][]GroupingScanner
}

// NewGroupContext creates a GroupingContext
func NewGroupContext(tags tag.Metas, scanners map[tag.KeyID][]GroupingScanner) GroupingContext {
	return &groupingContext{
		tags:     tags,
		scanners: scanners,
	}
}

// ScanTagValueIDs scans grouping context by high key/container of series ids,
// then returns grouped tag value ids for each tag key
func (g *groupingContext) ScanTagValueIDs(highKey uint16, container roaring.Container) []*roaring.Bitmap {
	result := make([]*roaring.Bitmap, len(g.tags))
	for i, groupingTag := range g.tags {
		scanners := g.scanners[groupingTag.ID]
		tagValues := roaring.New()
		result[i] = tagValues
		for _, scanner := range scanners {
			// get series ids/tag value ids mapping by high key
			lowContainer, tagValueIDs := scanner.GetSeriesAndTagValue(highKey)
			if lowContainer == nil {
				// high key not exist
				continue
			}
			// iterator all series ids after filtering
			it := lowContainer.PeekableIterator()
			idx := 0
			for it.HasNext() {
				seriesID := it.Next()
				if container.Contains(seriesID) {
					tagValues.Add(tagValueIDs[idx])
				}
				idx++
			}
		}
	}
	return result
}

// BuildGroup builds the grouped series ids by the high key of series id
// and the container includes low keys of series id.
func (g *groupingContext) BuildGroup(seriesIDHighKey uint16, lowSeriesIDs roaring.Container) (rs []*TagScanner) {
	rs = make([]*TagScanner, g.tags.Len())
	for tagIndex, groupingTag := range g.tags {
		scanners := g.scanners[groupingTag.ID]
		tagScanner := &TagScanner{}
		for _, scanner := range scanners {
			lowSeriesIDsFromStroage, tagValueIDs := scanner.GetSeriesAndTagValue(seriesIDHighKey)
			if lowSeriesIDsFromStroage == nil {
				// high key not exist
				continue
			}
			foundSeriesIDs := lowSeriesIDsFromStroage.And(lowSeriesIDs)
			if foundSeriesIDs.GetCardinality() == 0 {
				// series ids not match
				continue
			}
			tagScanner.mappings = append(tagScanner.mappings, NewSeriesID2TagValues(lowSeriesIDsFromStroage, tagValueIDs))
		}
		rs[tagIndex] = tagScanner
	}
	return
}

type SeriesID2TagValues struct {
	seriesIDs *LowSeriesIDs

	tagValueIDs []uint32
}

func NewSeriesID2TagValues(lowSeriesIDs roaring.Container, tagValueIDs []uint32) *SeriesID2TagValues {
	return &SeriesID2TagValues{
		seriesIDs:   NewLowSeriesIDs(lowSeriesIDs),
		tagValueIDs: tagValueIDs,
	}
}

func (m *SeriesID2TagValues) FindTagValue(seriesID uint16) (uint32, bool) {
	index, ok := m.seriesIDs.Find(seriesID)
	if ok {
		return m.tagValueIDs[index], true
	}
	return 0, false
}

type TagScanner struct {
	mappings []*SeriesID2TagValues
}

func (s *TagScanner) FindTagValue(seriesID uint16) (uint32, bool) {
	for _, mapping := range s.mappings {
		tagValue, ok := mapping.FindTagValue(seriesID)
		if ok {
			return tagValue, true
		}
	}
	return 0, false
}
