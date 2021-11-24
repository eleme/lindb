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

package constants

import (
	"fmt"
)

// defines the role type of node.
const (
	MasterRole  = "Master"
	BrokerRole  = "Broker"
	StorageRole = "Storage"
)

// defines common constants will be used in broker and storage.
const (
	// LiveNodesPath represents live nodes prefix path for node register.
	LiveNodesPath = "/live/nodes"
	// StateNodesPath represents the state of node that node will report runtime status
	//TODO need remove
	StateNodesPath = "/state/nodes"
)

// defines broker level constants will be used in broker.
const (
	// MasterPath represents master elect path.
	MasterPath = "/master/node"
	// DatabaseConfigPath represents database config path.
	DatabaseConfigPath = "/database/config"
	// ShardAssigmentPath represents database shard assignment.
	ShardAssigmentPath = "/database/assign"
	// StorageConfigPath represents storage cluster's config.
	StorageConfigPath = "/storage/config"
	// StorageStatePath represents storage cluster's state.
	StorageStatePath = "/storage/state"
)

// GetStorageClusterConfigPath returns path which storing config of storage cluster
func GetStorageClusterConfigPath(name string) string {
	return fmt.Sprintf("%s/%s", StorageConfigPath, name)
}

func GetStorageStatePath(name string) string {
	return fmt.Sprintf("%s/%s", StorageStatePath, name)
}

// GetDatabaseConfigPath returns path which storing config of database
func GetDatabaseConfigPath(name string) string {
	return fmt.Sprintf("%s/%s", DatabaseConfigPath, name)
}

// GetDatabaseAssignPath returns path which storing shard assignment of database
func GetDatabaseAssignPath(name string) string {
	return fmt.Sprintf("%s/%s", ShardAssigmentPath, name)
}

// GetLiveNodePath returns live node register path.
func GetLiveNodePath(node string) string {
	return fmt.Sprintf("%s/%s", LiveNodesPath, node)
}

// GetNodeMonitoringStatPath returns the node monitoring stat's path
func GetNodeMonitoringStatPath(node string) string {
	return fmt.Sprintf("%s/%s", StateNodesPath, node)
}
