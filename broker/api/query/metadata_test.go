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

package query

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/lindb/lindb/broker/deps"
	"github.com/lindb/lindb/config"
	"github.com/lindb/lindb/coordinator"
	"github.com/lindb/lindb/internal/mock"
	"github.com/lindb/lindb/models"
	"github.com/lindb/lindb/pkg/encoding"
	"github.com/lindb/lindb/pkg/ltoml"
	"github.com/lindb/lindb/query"
	"github.com/lindb/lindb/series/field"
	"github.com/lindb/lindb/service"
	"github.com/lindb/lindb/sql/stmt"
)

func TestMetadataAPI_Handle_err(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer func() {
		parseSQLFunc = parseSQL

		ctrl.Finish()
	}()

	api := NewMetadataAPI(&deps.HTTPDeps{})
	r := gin.New()
	api.Register(r)
	// case 1: database name not input
	resp := mock.DoRequest(t, r, http.MethodGet, MetadataQueryPath, "")
	assert.Equal(t, http.StatusInternalServerError, resp.Code)

	// case 2: parse sql err
	resp = mock.DoRequest(t, r, http.MethodGet, MetadataQueryPath+"?db=db&sql=show d", "")
	assert.Equal(t, http.StatusInternalServerError, resp.Code)

	// case 3: wrong type
	resp = mock.DoRequest(t, r, http.MethodGet, MetadataQueryPath+"?db=db&sql=select f1 from cpu", "")
	assert.Equal(t, http.StatusInternalServerError, resp.Code)

	// case 4: unknown metadata type
	parseSQLFunc = func(ql string) (*stmt.Metadata, error) {
		return &stmt.Metadata{}, nil
	}
	resp = mock.DoRequest(t, r, http.MethodGet, MetadataQueryPath+"?db=db&sql=select f1 from cpu", "")
	assert.Equal(t, http.StatusInternalServerError, resp.Code)
}

func TestMetadataAPI_ShowDatabases(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	databaseService := service.NewMockDatabaseService(ctrl)
	api := NewMetadataAPI(&deps.HTTPDeps{DatabaseSrv: databaseService})
	r := gin.New()
	api.Register(r)

	databaseService.EXPECT().List().Return(nil, nil)
	resp := mock.DoRequest(t, r, http.MethodGet, MetadataQueryPath+"?sql=show databases", "")
	assert.Equal(t, http.StatusOK, resp.Code)

	databaseService.EXPECT().List().Return(
		[]*models.Database{
			{Name: "test1"},
			{Name: "test2"},
		},
		nil)
	resp = mock.DoRequest(t, r, http.MethodGet, MetadataQueryPath+"?sql=show databases", "")
	assert.Equal(t, http.StatusOK, resp.Code)

	databaseService.EXPECT().List().Return(nil, fmt.Errorf("err"))
	resp = mock.DoRequest(t, r, http.MethodGet, MetadataQueryPath+"?sql=show databases", "")
	assert.Equal(t, http.StatusInternalServerError, resp.Code)
}

func TestMetadataAPI_SuggestCommon(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := query.NewMockFactory(ctrl)
	metaDataQuery := query.NewMockMetaDataQuery(ctrl)

	factory.EXPECT().NewMetadataQuery(gomock.Any(), gomock.Any(), gomock.Any()).Return(metaDataQuery).AnyTimes()

	api := NewMetadataAPI(
		&deps.HTTPDeps{
			StateMachines: &coordinator.BrokerStateMachines{},
			QueryFactory:  factory,
			BrokerCfg:     &config.BrokerBase{Query: config.Query{Timeout: ltoml.Duration(time.Second * 10)}},
		})
	r := gin.New()
	api.Register(r)

	resp := mock.DoRequest(t, r, http.MethodGet, MetadataQueryPath+"?sql=show namespaces", "")
	assert.Equal(t, http.StatusInternalServerError, resp.Code)

	metaDataQuery.EXPECT().WaitResponse().Return(nil, fmt.Errorf("err"))
	resp = mock.DoRequest(t, r, http.MethodGet, MetadataQueryPath+"?db=db&sql=show namespaces", "")
	assert.Equal(t, http.StatusInternalServerError, resp.Code)

	metaDataQuery.EXPECT().WaitResponse().Return([]string{"a", "b"}, nil)
	resp = mock.DoRequest(t, r, http.MethodGet, MetadataQueryPath+"?db=db&sql=show namespaces", "")
	assert.Equal(t, http.StatusOK, resp.Code)

	metaDataQuery.EXPECT().WaitResponse().Return([]string{"ddd"}, nil)
	resp = mock.DoRequest(t, r, http.MethodGet, MetadataQueryPath+"?db=db&sql=show fields from cpu", "")
	assert.Equal(t, http.StatusInternalServerError, resp.Code)

	metaDataQuery.EXPECT().WaitResponse().Return([]string{string(encoding.JSONMarshal(&[]field.Meta{{Name: "test", Type: field.SumField}}))}, nil)
	resp = mock.DoRequest(t, r, http.MethodGet, MetadataQueryPath+"?db=db&sql=show fields from cpu", "")
	assert.Equal(t, http.StatusOK, resp.Code)

	// histogram
	metaDataQuery.EXPECT().WaitResponse().Return([]string{string(encoding.JSONMarshal(&[]field.Meta{
		{Name: "test", Type: field.SumField},
		{Name: "histogram_0", Type: field.HistogramField},
		{Name: "histogram_2", Type: field.HistogramField},
		{Name: "histogram_3", Type: field.HistogramField},
		{Name: "histogram_4", Type: field.HistogramField},
		{Name: "histogram_99", Type: field.HistogramField},
		{Name: "histogram_sum", Type: field.SumField},
		{Name: "histogram_count", Type: field.SumField},
		{Name: "histogram_min", Type: field.MinField},
		{Name: "histogram_max", Type: field.MaxField},
	}))}, nil)
	resp = mock.DoRequest(t, r, http.MethodGet, MetadataQueryPath+"?db=db&sql=show fields from cpu", "")
	assert.Equal(t, http.StatusOK, resp.Code)
}
