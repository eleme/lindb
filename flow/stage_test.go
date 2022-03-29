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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStage_String(t *testing.T) {
	assert.Equal(t, "scanner", ScannerStage.String())
	assert.Equal(t, "filtering", FilteringStage.String())
	assert.Equal(t, "grouping", GroupingStage.String())
	assert.Equal(t, "downSampling", DownSamplingStage.String())
	assert.Equal(t, "unknown", Stage(99999).String())
}
