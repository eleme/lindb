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

package replica

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/atomic"

	"github.com/lindb/lindb/coordinator/broker"
	"github.com/lindb/lindb/models"
	"github.com/lindb/lindb/pkg/logger"
	"github.com/lindb/lindb/rpc"
	"github.com/lindb/lindb/series/metric"
)

//go:generate mockgen -source=./channel_manager.go -destination=./channel_manager_mock.go -package=replica

// ChannelManager manages the construction, retrieving, closing for all channels.
type ChannelManager interface {
	// Write writes a MetricList, the manager handler the database, sharding things.
	Write(ctx context.Context, database string, brokerBatchRows *metric.BrokerBatchRows) error
	// CreateChannel creates a new channel or returns a existed channel for storage with specific database and shardID,
	// numOfShard should be greater or equal than the origin setting, otherwise error is returned.
	// numOfShard is used eot calculate the shardID for a given hash.
	CreateChannel(databaseCfg models.Database, numOfShard int32, shardID models.ShardID) (Channel, error)

	// Close closes all the channel.
	Close()
}

// channelManager implements ChannelManager.
type (
	database2Channel map[string]DatabaseChannel
	databaseChannels struct {
		value atomic.Value // readonly database2Channel
		mu    sync.Mutex   // lock for modifying database2Channel
	}

	channelManager struct {
		// context passed to all Channel
		ctx context.Context
		// cancelFun to cancel context
		cancel context.CancelFunc
		// factory to get rpc  writeTask client
		fct      rpc.ClientStreamFactory
		stateMgr broker.StateManager

		databaseChannels databaseChannels

		logger *logger.Logger
	}
)

// NewChannelManager returns a ChannelManager with dirPath and WriteClientFactory.
// WriteClientFactory makes it easy to mock rpc streamClient for test.
func NewChannelManager(
	ctx context.Context,
	fct rpc.ClientStreamFactory,
	stateMgr broker.StateManager,
) ChannelManager {
	ctx, cancel := context.WithCancel(ctx)
	cm := &channelManager{
		ctx:      ctx,
		cancel:   cancel,
		fct:      fct,
		stateMgr: stateMgr,
		logger:   logger.GetLogger("replica", "ChannelManager"),
	}
	cm.databaseChannels.value.Store(make(database2Channel))

	stateMgr.WatchShardStateChangeEvent(cm.handleShardStateChangeEvent)
	return cm
}

// Write writes a MetricList, the manager handler the database, sharding things.
func (cm *channelManager) Write(ctx context.Context, database string, brokerBatchRows *metric.BrokerBatchRows) error {
	if brokerBatchRows == nil || brokerBatchRows.Len() == 0 {
		return nil
	}
	databaseChannel, ok := cm.getDatabaseChannel(database)
	if !ok {
		return fmt.Errorf("database [%s] not found", database)
	}
	return databaseChannel.Write(ctx, brokerBatchRows)
}

func (cm *channelManager) handleShardStateChangeEvent(
	databaseCfg models.Database,
	shards map[models.ShardID]models.ShardState,
	liveNodes map[models.NodeID]models.StatefulNode,
) {
	numOfShard := len(shards)
	for _, shardState := range shards {
		shardID := shardState.ID
		ch, err := cm.CreateChannel(databaseCfg, int32(numOfShard), shardID)
		if err != nil {
			cm.logger.Error("create shard write channel", logger.String("db", databaseCfg.Name),
				logger.Any("shard", shardID), logger.Error(err))
		} else {
			ch.SyncShardState(shardState, liveNodes)
		}
	}
}

// CreateChannel creates a new channel or returns a existed channel for storage with specific database and shardID.
// NumOfShard should be greater or equal than the origin setting, otherwise error is returned.
func (cm *channelManager) CreateChannel(databaseCfg models.Database, numOfShard int32, shardID models.ShardID) (Channel, error) {
	if numOfShard <= 0 || int32(shardID) >= numOfShard {
		return nil, errors.New("numOfShard should be greater than 0 and shardID should less then numOfShard")
	}
	database := databaseCfg.Name
	ch, ok := cm.getDatabaseChannel(database)
	if !ok {
		// double check, need lock
		cm.databaseChannels.mu.Lock()
		defer cm.databaseChannels.mu.Unlock()

		ch, ok = cm.getDatabaseChannel(database)
		if !ok {
			// if not exist, create database channel
			ch, err := newDatabaseChannel(cm.ctx, databaseCfg, numOfShard, cm.fct)
			if err != nil {
				return nil, err
			}
			// clone databases and creates a new map to hold database channels
			cm.insertDatabaseChannel(database, ch)

			cm.logger.Info("create shard write channel successfully",
				logger.String("db", database),
				logger.Int("shardID", shardID.Int()))

			// create shard level channel
			return ch.CreateChannel(numOfShard, shardID)
		}
	}
	return ch.CreateChannel(numOfShard, shardID)
}

// Close closes all the channel.
func (cm *channelManager) Close() {
	cm.cancel()

	// preventing creating new channels
	cm.databaseChannels.mu.Lock()
	defer cm.databaseChannels.mu.Unlock()

	channels := cm.databaseChannels.value.Load().(database2Channel)
	for _, channel := range channels {
		channel.Stop()
	}
}

// getDatabaseChannel gets the database channel by given database name
func (cm *channelManager) getDatabaseChannel(databaseName string) (DatabaseChannel, bool) {
	ch, ok := cm.databaseChannels.value.Load().(database2Channel)[databaseName]
	return ch, ok
}

func (cm *channelManager) insertDatabaseChannel(newDatabaseName string, newChannel DatabaseChannel) {
	oldMap := cm.databaseChannels.value.Load().(database2Channel)
	newMap := make(database2Channel)
	for databaseName, channel := range oldMap {
		newMap[databaseName] = channel
	}
	newMap[newDatabaseName] = newChannel
	cm.databaseChannels.value.Store(newMap)
}
