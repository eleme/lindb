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

package rpc

import (
	"io"
	"sync"

	"github.com/lindb/common/pkg/logger"

	"github.com/lindb/lindb/models"
)

//go:generate mockgen -source ./connection_manager.go -destination=./connection_manager_mock.go -package=rpc

// ConnectionManager represents grpc connection manager.
type ConnectionManager interface {
	io.Closer

	// CreateConnection creates a grpc connection.
	CreateConnection(target models.Node)
	// CloseConnection closes a grpc connection.
	CloseConnection(target models.Node)
}

// connectionManager implements ConnectionManager interface.
type connectionManager struct {
	connections map[string]struct{}

	mutex sync.Mutex

	logger logger.Logger
}

// NewConnectionManager creates a ConnectionManager instance.
func NewConnectionManager() ConnectionManager {
	return &connectionManager{
		connections: make(map[string]struct{}),
		logger:      logger.GetLogger("RPC", "ConnectionManager"),
	}
}

// CreateConnection creates a grpc connection, if success cache the connection.
func (m *connectionManager) CreateConnection(target models.Node) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	nodeID := target.Indicator()
	if _, ok := m.connections[nodeID]; ok {
		// connection exist, return it
		return
	}
}

// CloseConnection closes a grpc connection by given target server.
func (m *connectionManager) CloseConnection(target models.Node) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.closeConnection(target.Indicator())
}

// Close closes connection manager, clean all grpc connections.
func (m *connectionManager) Close() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for target := range m.connections {
		m.closeConnection(target)
	}
	return nil
}

// closeConnection closes a grpc connection, then clear the cache for target server.
func (m *connectionManager) closeConnection(target string) {
	// TODO: check connection closed?
	delete(m.connections, target)
}
