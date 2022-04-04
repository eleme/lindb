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
	"github.com/lindb/lindb/series"
)

//go:generate mockgen -source=./query_flow.go -destination=./query_flow_mock.go -package=flow

// StorageQueryFlow represents the storage query engine execute flow.
type StorageQueryFlow interface {
	// Prepare prepares the query flow, builds the flow execute context based on group aggregator specs.
	Prepare()
	// Submit submits an async task when do query pipeline.
	Submit(stage Stage, task func())
	// Reduce reduces the down sampling aggregator's result.
	Reduce(it series.GroupedIterator)
	// ReduceTagValues reduces the group by tag values.
	ReduceTagValues(tagKeyIndex int, tagValues map[uint32]string)
	// Complete completes the query flow with error.
	Complete(err error)
}

// QueryTask represents query task for data search flow.
type QueryTask interface {
	// BeforeRun invokes before task run.
	BeforeRun()
	// Run executes task query logic.
	Run() error
	// AfterRun invokes after task run.
	AfterRun()
}
