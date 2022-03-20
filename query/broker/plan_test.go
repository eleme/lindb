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

package brokerquery

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lindb/lindb/models"
	"github.com/lindb/lindb/pkg/option"
	"github.com/lindb/lindb/query"
	"github.com/lindb/lindb/sql"
	"github.com/lindb/lindb/sql/stmt"
)

func TestBrokerPlan_Wrong_Case(t *testing.T) {
	plan := newBrokerPlan(&stmt.Query{}, models.Database{}, nil, models.StatelessNode{}, nil)
	// storage nodes cannot be empty
	err := plan.Plan()
	assert.Equal(t, query.ErrNoAvailableStorageNode, err)
}

func TestBrokerPlan_No_GroupBy(t *testing.T) {
	storageNodes := map[string][]models.ShardID{"1.1.1.1:9000": {1, 2, 4}, "1.1.1.2:9000": {3, 5, 6}}
	currentNode := generateBrokerActiveNode("1.1.1.3", 8000)
	q, err := sql.Parse("select f from cpu")
	assert.NoError(t, err)
	// no group sql
	plan := newBrokerPlan(q.(*stmt.Query),
		models.Database{Option: &option.DatabaseOption{Intervals: option.Intervals{{Interval: 10 * 100}}}},
		storageNodes, currentNode, nil)
	err = plan.Plan()
	assert.NoError(t, err)

	assert.Equal(t, 0, len(plan.intermediateNodes))
	physicalPlan := models.NewPhysicalPlan(models.Root{Indicator: "1.1.1.3:8000", NumOfTask: 2})
	leaf1 := &models.Leaf{
		BaseNode: models.BaseNode{
			Parent:    "1.1.1.3:8000",
			Indicator: "1.1.1.1:9000",
		},
		Receivers: []models.StatelessNode{currentNode},
		ShardIDs:  []models.ShardID{1, 2, 4},
	}
	physicalPlan.AddLeaf(leaf1)
	leaf2 := &models.Leaf{
		BaseNode: models.BaseNode{
			Parent:    "1.1.1.3:8000",
			Indicator: "1.1.1.2:9000",
		},
		Receivers: []models.StatelessNode{currentNode},
		ShardIDs:  []models.ShardID{3, 5, 6},
	}
	physicalPlan.AddLeaf(leaf2)

	assert.Equal(t, physicalPlan.Root, plan.physicalPlan.Root)
	assert.Equal(t, 2, len(plan.physicalPlan.Leaves))
	assert.Equal(t, 0, len(plan.physicalPlan.Intermediates))
}

func TestBrokerPlan_GroupBy_oddCount(t *testing.T) {
	// odd number
	oddStorageNodes := map[string][]models.ShardID{
		"1.1.1.1:9000": {1, 2, 4},
		"1.1.1.2:9000": {3, 6, 9},
		"1.1.1.3:9000": {5, 7, 8},
		"1.1.1.4:9000": {10, 13, 15},
		"1.1.1.5:9000": {11, 12, 14},
	}
	q, err := sql.Parse("select f from cpu group by host")
	assert.NoError(t, err)
	currentNode := generateBrokerActiveNode("1.1.1.3", 8000)
	opt := &option.DatabaseOption{Intervals: option.Intervals{{Interval: 10 * 100}}}
	plan := newBrokerPlan(
		q.(*stmt.Query),
		models.Database{Option: opt},
		oddStorageNodes,
		currentNode,
		[]models.StatelessNode{
			generateBrokerActiveNode("1.1.1.1", 8000),
			generateBrokerActiveNode("1.1.1.2", 8000),
			currentNode,
			generateBrokerActiveNode("1.1.1.4", 8000),
		})
	err = plan.Plan()
	assert.NoError(t, err)

	assert.Equal(t, 3, len(plan.intermediateNodes))
	physicalPlan := plan.physicalPlan
	assert.Equal(t, models.Root{Indicator: "1.1.1.3:8000", NumOfTask: 3}, physicalPlan.Root)
	assert.Equal(t, 3, len(physicalPlan.Intermediates))
	for _, intermediate := range physicalPlan.Intermediates {
		assert.Equal(t, "1.1.1.3:8000", intermediate.Parent)
		assert.Equal(t, int32(5), intermediate.NumOfTask)
	}
	assert.Equal(t, 5, len(physicalPlan.Leaves))
	storageNodes2 := make(map[string][]models.ShardID)
	for _, leaf := range physicalPlan.Leaves {
		storageNodes2[leaf.Indicator] = leaf.ShardIDs
		assert.Equal(t, 3, len(leaf.Receivers))
	}
	assert.Equal(t, oddStorageNodes, storageNodes2)
}

func TestBrokerPlan_GroupBy_evenCount(t *testing.T) {
	// even number
	evenStorageNodes :=
		map[string][]models.ShardID{
			"1.1.1.4:9000": {10, 13, 15},
			"1.1.1.5:9000": {11, 12, 14},
		}
	currentNode := generateBrokerActiveNode("1.1.1.3", 8000)
	q, err := sql.Parse("select f from cpu group by host")
	assert.NoError(t, err)
	opt := &option.DatabaseOption{Intervals: option.Intervals{{Interval: 10 * 100}}}
	plan := newBrokerPlan(
		q.(*stmt.Query),
		models.Database{Option: opt},
		evenStorageNodes,
		currentNode,
		[]models.StatelessNode{
			generateBrokerActiveNode("1.1.1.2", 8000),
			currentNode,
			generateBrokerActiveNode("1.1.1.4", 8000),
		})
	err = plan.Plan()
	assert.NoError(t, err)

	assert.Equal(t, 2, len(plan.intermediateNodes))
	physicalPlan := plan.physicalPlan
	assert.Equal(t, models.Root{Indicator: "1.1.1.3:8000", NumOfTask: 2}, physicalPlan.Root)
	assert.Equal(t, 2, len(physicalPlan.Intermediates))
	for _, intermediate := range physicalPlan.Intermediates {
		assert.Equal(t, "1.1.1.3:8000", intermediate.Parent)
		assert.Equal(t, int32(2), intermediate.NumOfTask)
	}
	assert.Equal(t, 2, len(physicalPlan.Leaves))
	storageNodes2 := make(map[string][]models.ShardID)
	for _, leaf := range physicalPlan.Leaves {
		storageNodes2[leaf.Indicator] = leaf.ShardIDs
		assert.Equal(t, 2, len(leaf.Receivers))
	}
	assert.Equal(t, evenStorageNodes, storageNodes2)
}

func TestBrokerPlan_GroupBy_Less_StorageNodes(t *testing.T) {
	storageNodes := map[string][]models.ShardID{
		"1.1.1.1:9000": {1, 2, 4},
		"1.1.1.2:9000": {3, 5, 6},
	}
	currentNode := generateBrokerActiveNode("1.1.1.3", 8000)
	q, err := sql.Parse("select f from cpu group by host")
	assert.NoError(t, err)
	opt := &option.DatabaseOption{Intervals: option.Intervals{{Interval: 10 * 100}}}
	plan := newBrokerPlan(
		q.(*stmt.Query),
		models.Database{Option: opt},
		storageNodes,
		currentNode,
		[]models.StatelessNode{
			generateBrokerActiveNode("1.1.1.1", 8000),
			generateBrokerActiveNode("1.1.1.2", 8000),
			currentNode,
			generateBrokerActiveNode("1.1.1.4", 8000),
		})
	err = plan.Plan()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 3, len(plan.intermediateNodes))
	physicalPlan := plan.physicalPlan
	assert.Equal(t, models.Root{Indicator: "1.1.1.3:8000", NumOfTask: 3}, physicalPlan.Root)
	assert.Equal(t, 3, len(physicalPlan.Intermediates))
	for _, intermediate := range physicalPlan.Intermediates {
		assert.Equal(t, "1.1.1.3:8000", intermediate.Parent)
		assert.Equal(t, int32(2), intermediate.NumOfTask)
	}
	assert.Equal(t, 2, len(physicalPlan.Leaves))
	storageNodes2 := make(map[string][]models.ShardID)
	for _, leaf := range physicalPlan.Leaves {
		storageNodes2[leaf.Indicator] = leaf.ShardIDs
		assert.Equal(t, 3, len(leaf.Receivers))
	}
	assert.Equal(t, storageNodes, storageNodes2)
}

func TestBrokerPlan_GroupBy_Same_Broker(t *testing.T) {
	storageNodes := map[string][]models.ShardID{"1.1.1.1:9000": {1, 2, 4}}
	currentNode := generateBrokerActiveNode("1.1.1.3", 8000)

	q, err := sql.Parse("select f from cpu group by host")
	assert.NoError(t, err)
	// current node = active node
	opt := &option.DatabaseOption{Intervals: option.Intervals{{Interval: 10 * 100}}}
	plan := newBrokerPlan(
		q.(*stmt.Query),
		models.Database{Option: opt},
		storageNodes,
		currentNode,
		[]models.StatelessNode{currentNode})
	err = plan.Plan()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 0, len(plan.intermediateNodes))
	physicalPlan := models.NewPhysicalPlan(models.Root{Indicator: "1.1.1.3:8000", NumOfTask: 1})
	leaf := &models.Leaf{
		BaseNode: models.BaseNode{
			Parent:    "1.1.1.3:8000",
			Indicator: "1.1.1.1:9000",
		},
		Receivers: []models.StatelessNode{currentNode},
		ShardIDs:  []models.ShardID{1, 2, 4},
	}
	physicalPlan.AddLeaf(leaf)
	assert.Equal(t, physicalPlan, plan.physicalPlan)
}

func TestBrokerPlan_GroupBy_No_Broker(t *testing.T) {
	storageNodes := map[string][]models.ShardID{"1.1.1.1:9000": {1, 2, 4}}
	currentNode := generateBrokerActiveNode("1.1.1.3", 8000)

	// only one storage node
	q, err := sql.Parse("select f from cpu group by host")
	assert.NoError(t, err)
	opt := &option.DatabaseOption{Intervals: option.Intervals{{Interval: 10 * 100}}}
	plan := newBrokerPlan(
		q.(*stmt.Query),
		models.Database{Option: opt},
		storageNodes,
		currentNode,
		nil)
	err = plan.Plan()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 0, len(plan.intermediateNodes))
	physicalPlan := models.NewPhysicalPlan(models.Root{Indicator: "1.1.1.3:8000", NumOfTask: 1})
	leaf := &models.Leaf{
		BaseNode: models.BaseNode{
			Parent:    "1.1.1.3:8000",
			Indicator: "1.1.1.1:9000",
		},
		Receivers: []models.StatelessNode{currentNode},
		ShardIDs:  []models.ShardID{1, 2, 4},
	}
	physicalPlan.AddLeaf(leaf)
	assert.Equal(t, physicalPlan, plan.physicalPlan)
}

func TestBrokerPlan_GroupBy_One_StorageNode(t *testing.T) {
	storageNodes := map[string][]models.ShardID{"1.1.1.1:9000": {1, 2, 4}}
	currentNode := generateBrokerActiveNode("1.1.1.3", 8000)

	q, err := sql.Parse("select f from cpu group by host")
	assert.NoError(t, err)
	// only one storage node
	opt := &option.DatabaseOption{Intervals: option.Intervals{{Interval: 10 * 100}}}
	plan := newBrokerPlan(
		q.(*stmt.Query),
		models.Database{Option: opt},
		storageNodes,
		currentNode,
		[]models.StatelessNode{
			generateBrokerActiveNode("1.1.1.1", 8000),
			generateBrokerActiveNode("1.1.1.2", 8100),
			currentNode,
			generateBrokerActiveNode("1.1.1.4", 8200),
		})
	err = plan.Plan()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 0, len(plan.intermediateNodes))
	physicalPlan := models.NewPhysicalPlan(models.Root{Indicator: "1.1.1.3:8000", NumOfTask: 1})
	leaf := &models.Leaf{
		BaseNode: models.BaseNode{
			Parent:    "1.1.1.3:8000",
			Indicator: "1.1.1.1:9000",
		},
		Receivers: []models.StatelessNode{currentNode},
		ShardIDs:  []models.ShardID{1, 2, 4},
	}
	physicalPlan.AddLeaf(leaf)
	assert.Equal(t, physicalPlan, plan.physicalPlan)
}

func generateBrokerActiveNode(ip string, port int) models.StatelessNode {
	return models.StatelessNode{HostIP: ip, GRPCPort: uint16(port)}
}
