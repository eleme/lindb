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

package storagequery

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/lindb/roaring"
	"github.com/stretchr/testify/assert"

	"github.com/lindb/lindb/flow"
	"github.com/lindb/lindb/models"
	"github.com/lindb/lindb/pkg/encoding"
	"github.com/lindb/lindb/series"
	"github.com/lindb/lindb/series/field"
	"github.com/lindb/lindb/series/tag"
	"github.com/lindb/lindb/sql/stmt"
	"github.com/lindb/lindb/tsdb"
	"github.com/lindb/lindb/tsdb/indexdb"
	"github.com/lindb/lindb/tsdb/metadb"
)

func TestMetadataStorageQuery_Execute(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	db := tsdb.NewMockDatabase(ctrl)

	metadata := metadb.NewMockMetadata(ctrl)
	db.EXPECT().Metadata().Return(metadata).AnyTimes()
	metadataIndex := metadb.NewMockMetadataDatabase(ctrl)
	metadata.EXPECT().MetadataDatabase().Return(metadataIndex).AnyTimes()

	// case 1: suggest namespace
	exec := newStorageMetadataQuery(db, nil, &stmt.MetricMetadata{
		Type: stmt.Namespace,
	})
	metadataIndex.EXPECT().SuggestNamespace(gomock.Any(), gomock.Any()).Return([]string{"a"}, nil)
	result, err := exec.Execute()
	assert.NoError(t, err)
	assert.Equal(t, []string{"a"}, result)

	// case 2: suggest metric name
	exec = newStorageMetadataQuery(db, nil, &stmt.MetricMetadata{
		Type: stmt.Metric,
	})
	metadataIndex.EXPECT().SuggestMetrics(gomock.Any(), gomock.Any(), gomock.Any()).Return([]string{"a"}, nil)
	result, err = exec.Execute()
	assert.NoError(t, err)
	assert.Equal(t, []string{"a"}, result)

	// case 3: suggest tag keys
	exec = newStorageMetadataQuery(db, nil, &stmt.MetricMetadata{
		Type: stmt.TagKey,
	})
	metadataIndex.EXPECT().GetAllTagKeys(gomock.Any(), gomock.Any()).Return(tag.Metas{{Key: "a"}}, nil)
	result, err = exec.Execute()
	assert.NoError(t, err)
	assert.Equal(t, []string{"a"}, result)
	exec = newStorageMetadataQuery(db, nil, &stmt.MetricMetadata{
		Type: stmt.TagKey,
	})
	metadataIndex.EXPECT().GetAllTagKeys(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("err"))
	result, err = exec.Execute()
	assert.Error(t, err)
	assert.Empty(t, result)
	// case 4: get fields err
	exec = newStorageMetadataQuery(db, nil, &stmt.MetricMetadata{
		Type: stmt.Field,
	})
	metadataIndex.EXPECT().GetAllFields(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("err"))
	result, err = exec.Execute()
	assert.Error(t, err)
	assert.Empty(t, result)

	// case 5: get fields
	exec = newStorageMetadataQuery(db, nil, &stmt.MetricMetadata{
		Type: stmt.Field,
	})
	metadataIndex.EXPECT().GetAllFields(gomock.Any(), gomock.Any()).Return([]field.Meta{{ID: 10}}, nil)
	result, err = exec.Execute()
	assert.NoError(t, err)
	assert.Equal(t, string(encoding.JSONMarshal([]field.Meta{{ID: 10}})), result[0])

	// case 6: suggest tag values
	exec = newStorageMetadataQuery(db, []models.ShardID{1, 2}, &stmt.MetricMetadata{
		Type: stmt.TagValue,
	})
	metadataIndex.EXPECT().GetTagKeyID(gomock.Any(), gomock.Any(), gomock.Any()).Return(tag.KeyID(2), nil)

	tagMeta := metadb.NewMockTagMetadata(ctrl)
	metadata.EXPECT().TagMetadata().Return(tagMeta).AnyTimes()

	tagMeta.EXPECT().SuggestTagValues(gomock.Any(), gomock.Any(), gomock.Any()).Return([]string{"a"})
	result, err = exec.Execute()
	assert.NoError(t, err)
	assert.Equal(t, []string{"a"}, result)

	// case 7: suggest tag values err
	exec = newStorageMetadataQuery(db, []models.ShardID{1, 2}, &stmt.MetricMetadata{
		Type: stmt.TagValue,
	})
	metadataIndex.EXPECT().GetTagKeyID(gomock.Any(), gomock.Any(), gomock.Any()).Return(tag.EmptyTagKeyID, fmt.Errorf("err"))

	result, err = exec.Execute()
	assert.Error(t, err)
	assert.Empty(t, result)
}

func TestMetadataStorageQuery_Execute_With_Tag_Condition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer func() {
		newTagSearchFunc = newTagSearch
		newSeriesSearchFunc = newSeriesSearch

		ctrl.Finish()
	}()

	db := tsdb.NewMockDatabase(ctrl)

	metadata := metadb.NewMockMetadata(ctrl)
	db.EXPECT().Metadata().Return(metadata).AnyTimes()
	metadataIndex := metadb.NewMockMetadataDatabase(ctrl)
	metadata.EXPECT().MetadataDatabase().Return(metadataIndex).AnyTimes()
	metadataIndex.EXPECT().GetTagKeyID(gomock.Any(), gomock.Any(), gomock.Any()).Return(tag.KeyID(2), nil).AnyTimes()

	// case 1: tag search err
	tagSearch := NewMockTagSearch(ctrl)
	newTagSearchFunc = func(ctx *executeContext) TagSearch {
		return tagSearch
	}
	exec := newStorageMetadataQuery(db, []models.ShardID{1}, &stmt.MetricMetadata{
		Type:      stmt.TagValue,
		Condition: &stmt.EqualsExpr{},
		Limit:     2,
	})
	tagSearch.EXPECT().Filter().Return(fmt.Errorf("err"))
	_, err := exec.Execute()
	assert.Error(t, err)
	// case 2: tag not found
	tagSearch.EXPECT().Filter().Return(nil)
	_, err = exec.Execute()
	assert.Error(t, err)

	shard := tsdb.NewMockShard(ctrl)
	indexDB := indexdb.NewMockIndexDatabase(ctrl)
	shard.EXPECT().IndexDatabase().Return(indexDB).AnyTimes()

	newTagSearchFunc = func(ctx *executeContext) TagSearch {
		ctx.storageExecuteCtx.TagFilterResult = map[string]*flow.TagFilterResult{"key": {}}
		return tagSearch
	}
	tagSearch.EXPECT().Filter().Return(nil).AnyTimes()

	// case: shard not found
	db.EXPECT().GetShard(gomock.Any()).Return(nil, false)

	db.EXPECT().GetShard(gomock.Any()).Return(shard, true).AnyTimes()
	result, err := exec.Execute()
	assert.NoError(t, err)
	assert.Empty(t, result)

	// case 3: series search err
	seriesSearch := NewMockSeriesSearch(ctrl)
	newSeriesSearchFunc = func(filter series.Filter, filterResult map[string]*flow.TagFilterResult, condition stmt.Expr) SeriesSearch {
		return seriesSearch
	}
	seriesSearch.EXPECT().Search().Return(nil, fmt.Errorf("err"))
	_, err = exec.Execute()
	assert.Error(t, err)

	seriesSearch.EXPECT().Search().Return(roaring.BitmapOf(1, 2, 3), nil).AnyTimes()
	// case 4: get grouping err
	indexDB.EXPECT().GetGroupingContext(gomock.Any()).Return(fmt.Errorf("err"))
	_, err = exec.Execute()
	assert.Error(t, err)

	gCtx := flow.NewMockGroupingContext(ctrl)

	indexDB.EXPECT().GetGroupingContext(gomock.Any()).DoAndReturn(func(ctx *flow.ShardExecuteContext) error {
		ctx.GroupingContext = gCtx
		return nil
	}).AnyTimes()
	gCtx.EXPECT().ScanTagValueIDs(gomock.Any(), gomock.Any()).
		Return([]*roaring.Bitmap{roaring.BitmapOf(1, 2, 3)}).AnyTimes()
	tagMeta := metadb.NewMockTagMetadata(ctrl)
	metadata.EXPECT().TagMetadata().Return(tagMeta).AnyTimes()

	// case 5: collect tag value err
	tagMeta.EXPECT().CollectTagValues(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("err"))
	_, err = exec.Execute()
	assert.Error(t, err)

	// case 6: collect tag values
	tagMeta.EXPECT().CollectTagValues(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(tagKeyID tag.KeyID,
			tagValueIDs *roaring.Bitmap,
			tagValues map[uint32]string,
		) error {
			tagValues[12] = "a"
			tagValues[13] = "b"
			tagValues[14] = "c"
			tagValues[15] = "d"
			return nil
		})
	result, err = exec.Execute()
	assert.NoError(t, err)
	assert.Len(t, result, 2)
}
