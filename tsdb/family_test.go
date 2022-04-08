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

package tsdb

import (
	"fmt"
	"io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/lindb/lindb/flow"
	"github.com/lindb/lindb/kv"
	"github.com/lindb/lindb/kv/table"
	"github.com/lindb/lindb/kv/version"
	"github.com/lindb/lindb/models"
	"github.com/lindb/lindb/pkg/timeutil"
	"github.com/lindb/lindb/series/metric"
	stmtpkg "github.com/lindb/lindb/sql/stmt"
	"github.com/lindb/lindb/tsdb/tblstore/metricsdata"
)

func TestDataFamily_BaseTime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	family := kv.NewMockFamily(ctrl)
	timeRange := timeutil.TimeRange{
		Start: 10,
		End:   50,
	}
	database := NewMockDatabase(ctrl)
	database.EXPECT().Name().Return("test").AnyTimes()
	snapshot := version.NewMockSnapshot(ctrl)
	v := version.NewMockVersion(ctrl)
	v.EXPECT().GetSequences().Return(map[int32]int64{1: 10})
	snapshot.EXPECT().GetCurrent().Return(v)
	snapshot.EXPECT().Close()
	family.EXPECT().GetSnapshot().Return(snapshot)
	shard := NewMockShard(ctrl)
	shard.EXPECT().Database().Return(database)
	shard.EXPECT().ShardID().Return(models.ShardID(1))
	dataFamily := newDataFamily(shard, timeutil.Interval(timeutil.OneSecond*10), timeRange, 10, family)
	assert.Equal(t, timeRange, dataFamily.TimeRange())
	assert.Equal(t, timeutil.Interval(10000), dataFamily.Interval())
	assert.NotNil(t, dataFamily.Family())

	err := dataFamily.Close()
	assert.NoError(t, err)
}

func TestDataFamily_Filter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer func() {
		ctrl.Finish()
		newReaderFunc = metricsdata.NewReader
		newFilterFunc = metricsdata.NewFilter
	}()

	database := NewMockDatabase(ctrl)
	database.EXPECT().Name().Return("test").AnyTimes()
	family := kv.NewMockFamily(ctrl)
	snapshot := version.NewMockSnapshot(ctrl)
	snapshot.EXPECT().Close().AnyTimes()
	family.EXPECT().GetSnapshot().Return(snapshot).AnyTimes()
	timeRange := timeutil.TimeRange{
		Start: 10,
		End:   50,
	}
	v := version.NewMockVersion(ctrl)
	v.EXPECT().GetSequences().Return(map[int32]int64{1: 10})
	snapshot.EXPECT().GetCurrent().Return(v)
	shard := NewMockShard(ctrl)
	shard.EXPECT().Database().Return(database)
	shard.EXPECT().ShardID().Return(models.ShardID(1))
	dataFamily := newDataFamily(shard, timeutil.Interval(timeutil.OneSecond*10), timeRange, 10, family)

	// test find kv readers err
	snapshot.EXPECT().FindReaders(gomock.Any()).Return(nil, fmt.Errorf("err"))
	rs, err := dataFamily.Filter(&flow.ShardExecuteContext{
		StorageExecuteCtx: &flow.StorageExecuteContext{
			MetricID: metric.ID(10),
			Query: &stmtpkg.Query{
				TimeRange: timeutil.TimeRange{},
			},
		},
	})
	assert.Error(t, err)
	assert.Nil(t, rs)

	// case 1: find kv readers nil
	snapshot.EXPECT().FindReaders(gomock.Any()).Return(nil, nil)
	rs, err = dataFamily.Filter(&flow.ShardExecuteContext{
		StorageExecuteCtx: &flow.StorageExecuteContext{
			MetricID: metric.ID(10),
			Query: &stmtpkg.Query{
				TimeRange: timeutil.TimeRange{},
			},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, rs)

	// case 2: not find in reader
	reader := table.NewMockReader(ctrl)
	reader.EXPECT().Path().Return("test_path").AnyTimes()
	snapshot.EXPECT().FindReaders(gomock.Any()).Return([]table.Reader{reader}, nil)
	reader.EXPECT().Get(gomock.Any()).Return(nil, io.EOF)
	rs, err = dataFamily.Filter(&flow.ShardExecuteContext{
		StorageExecuteCtx: &flow.StorageExecuteContext{
			MetricID: metric.ID(10),
			Query: &stmtpkg.Query{
				TimeRange: timeutil.TimeRange{},
			},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, rs)

	// case 3: new metric reader err
	newReaderFunc = func(file string, buf []byte) (reader metricsdata.MetricReader, err error) {
		return nil, fmt.Errorf("err")
	}
	snapshot.EXPECT().FindReaders(gomock.Any()).Return([]table.Reader{reader}, nil)
	reader.EXPECT().Get(gomock.Any()).Return([]byte{1, 2, 3}, nil)
	rs, err = dataFamily.Filter(&flow.ShardExecuteContext{
		StorageExecuteCtx: &flow.StorageExecuteContext{
			MetricID: metric.ID(10),
			Query: &stmtpkg.Query{
				TimeRange: timeutil.TimeRange{},
			},
		},
	})
	assert.Error(t, err)
	assert.Nil(t, rs)

	// case 4: normal case
	newReaderFunc = func(file string, buf []byte) (reader metricsdata.MetricReader, err error) {
		return nil, nil
	}
	filter := metricsdata.NewMockFilter(ctrl)
	newFilterFunc = func(familyTime int64, snapshot version.Snapshot, readers []metricsdata.MetricReader) metricsdata.Filter {
		return filter
	}
	snapshot.EXPECT().FindReaders(gomock.Any()).Return([]table.Reader{reader}, nil)
	reader.EXPECT().Get(gomock.Any()).Return([]byte{1, 2, 3}, nil)
	filter.EXPECT().Filter(gomock.Any(), gomock.Any()).Return(nil, nil)
	_, err = dataFamily.Filter(&flow.ShardExecuteContext{
		StorageExecuteCtx: &flow.StorageExecuteContext{
			MetricID: metric.ID(10),
			Query: &stmtpkg.Query{
				TimeRange: timeutil.TimeRange{},
			},
		},
	})
	assert.NoError(t, err)

	err = dataFamily.Close()
	assert.NoError(t, err)
}
