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
	"io"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/lindb/lindb/config"
	"github.com/lindb/lindb/models"
	"github.com/lindb/lindb/pkg/logger"
	"github.com/lindb/lindb/pkg/timeutil"
	"github.com/lindb/lindb/rpc"
	"github.com/lindb/lindb/series/metric"
)

//go:generate mockgen -source=./channel_family.go -destination=./channel_family_mock.go -package=replica

// FamilyChannel represents family write shardChannel.
type FamilyChannel interface {
	// Write writes the data into the shardChannel,
	// ErrCanceled is returned when the shardChannel is canceled before data is written successfully.
	// Concurrent safe.
	Write(ctx context.Context, rows []metric.BrokerRow) error
	// leaderChanged notifies family shardChannel need change leader send stream
	leaderChanged(shardState models.ShardState,
		liveNodes map[models.NodeID]models.StatefulNode)
	// Stop stops the family shardChannel.
	Stop()
	// FamilyTime returns the family time of current shardChannel.
	FamilyTime() int64
	// isExpire returns if current family is expired.
	isExpire(ahead, behind int64) bool
}

// familyChannel implements FamilyChannel interface.
type familyChannel struct {
	// context to close shardChannel
	ctx        context.Context
	cancel     context.CancelFunc
	database   string
	shardID    models.ShardID
	familyTime int64

	newWriteStreamFn func(
		ctx context.Context,
		target models.Node,
		database string, shardState *models.ShardState, familyTime int64,
		fct rpc.ClientStreamFactory,
	) (rpc.WriteStream, error)

	fct           rpc.ClientStreamFactory
	shardState    models.ShardState
	liveNodes     map[models.NodeID]models.StatefulNode
	currentTarget models.Node

	// shardChannel to convert multiple goroutine writeTask to single goroutine writeTask to FanOutQueue
	ch                  chan *compressedChunk
	leaderChangedSignal chan struct{}
	chunk               Chunk // buffer current writeTask metric for compress

	lastFlushTime      *atomic.Int64 // last flush time
	checkFlushInterval time.Duration // interval for check flush
	batchTimout        time.Duration // interval for flush
	maxRetryBuf        int

	lock4write sync.Mutex
	lock4meta  sync.Mutex
	logger     *logger.Logger
}

func newFamilyChannel(
	ctx context.Context,
	cfg config.Write,
	database string,
	shardID models.ShardID,
	familyTime int64,
	fct rpc.ClientStreamFactory,
	shardState models.ShardState,
	liveNodes map[models.NodeID]models.StatefulNode,
) FamilyChannel {
	c, cancel := context.WithCancel(ctx)
	fc := &familyChannel{
		ctx:                 c,
		cancel:              cancel,
		database:            database,
		shardID:             shardID,
		familyTime:          familyTime,
		fct:                 fct,
		shardState:          shardState,
		liveNodes:           liveNodes,
		newWriteStreamFn:    rpc.NewWriteStream,
		ch:                  make(chan *compressedChunk, 2),
		leaderChangedSignal: make(chan struct{}, 1),
		checkFlushInterval:  time.Second,
		batchTimout:         cfg.BatchTimeout.Duration(),
		maxRetryBuf:         100, // TODO add config
		chunk:               newChunk(cfg.BatchBlockSize),
		lastFlushTime:       atomic.NewInt64(timeutil.Now()),
		logger:              logger.GetLogger("replica", "FamilyChannel"),
	}

	activeWriteFamilies.WithTagValues(database).Incr()

	go fc.writeTask()

	return fc
}

// Write writes the data into the shardChannel, ErrCanceled is returned when the ctx is canceled before
// data is written successfully.
// Concurrent safe.
func (fc *familyChannel) Write(ctx context.Context, rows []metric.BrokerRow) error {
	total := len(rows)
	success := 0

	fc.lock4write.Lock()
	defer func() {
		if total > 0 {
			batchMetrics.WithTagValues(fc.database).Add(float64(success))
			batchMetricFailures.WithTagValues(fc.database).Add(float64(total - success))
		}
		fc.lock4write.Unlock()
	}()

	for idx := 0; idx < total; idx++ {
		if _, err := rows[idx].WriteTo(fc.chunk); err != nil {
			return err
		}

		if err := fc.flushChunkOnFull(ctx); err != nil {
			return err
		}
		success++
	}

	return nil
}

// leaderChanged notifies family shardChannel need change leader send stream
func (fc *familyChannel) leaderChanged(
	shardState models.ShardState,
	liveNodes map[models.NodeID]models.StatefulNode,
) {
	fc.lock4meta.Lock()
	fc.shardState = shardState
	fc.liveNodes = liveNodes
	fc.lock4meta.Unlock()

	fc.leaderChangedSignal <- struct{}{}
}

func (fc *familyChannel) flushChunkOnFull(ctx context.Context) error {
	if !fc.chunk.IsFull() {
		return nil
	}
	compressed, err := fc.chunk.Compress()
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done(): // timeout of http ingestion api
		return ErrIngestTimeout
	case <-fc.ctx.Done():
		return ErrFamilyChannelCanceled
	case fc.ch <- compressed:
		return nil
	}
}

// writeTask consumes data from chan, then appends the data into queue
func (fc *familyChannel) writeTask() {
	// on avg 2 * limit could avoid buffer grow
	ticker := time.NewTicker(fc.checkFlushInterval)
	defer ticker.Stop()

	retryBuffers := make([]*compressedChunk, 0)
	retry := func(compressed *compressedChunk) {
		if len(retryBuffers) > fc.maxRetryBuf {
			fc.logger.Error("too many retry messages, drop current message")
			retryDrop.WithTagValues(fc.database).Incr()
		} else {
			retryBuffers = append(retryBuffers, compressed)
			retryCount.WithTagValues(fc.database).Incr()
		}
	}
	var stream rpc.WriteStream
	send := func(compressed *compressedChunk) bool {
		if compressed == nil {
			return true
		}
		if len(*compressed) == 0 {
			compressed.Release()
			return true
		}
		if stream == nil {
			fc.lock4meta.Lock()
			leader := fc.liveNodes[fc.shardState.Leader]
			shardState := fc.shardState
			fc.currentTarget = &leader
			fc.lock4meta.Unlock()
			s, err := fc.newWriteStreamFn(fc.ctx, fc.currentTarget, fc.database, &shardState, fc.familyTime, fc.fct)
			if err != nil {
				retry(compressed)
				return false
			}
			stream = s
		}
		if err := stream.Send(*compressed); err != nil {
			sendFailure.WithTagValues(fc.database).Incr()
			fc.logger.Error(
				"failed writing compressed chunk to storage",
				logger.String("target", fc.currentTarget.Indicator()),
				logger.String("database", fc.database),
				logger.Error(err))
			if err == io.EOF {
				if closeError := stream.Close(); closeError != nil {
					fc.logger.Error("failed closing write stream",
						logger.String("target", fc.currentTarget.Indicator()),
						logger.Error(closeError))
				}
				stream = nil
			}
			// retry if err
			retry(compressed)
			return false
		}
		sendSuccess.WithTagValues(fc.database).Incr()
		sendSize.WithTagValues(fc.database).Add(float64(len(*compressed)))
		pendingSend.WithTagValues(fc.database).Decr()
		compressed.Release()
		return true
	}

	defer func() {
		if stream != nil {
			if err := stream.Close(); err != nil {
				fc.logger.Error("close write stream err when exit write task", logger.Error(err))
			}
		}
	}()
	var err error

	for {
		select {
		case <-fc.ctx.Done():
			sendLastMsg := func(compressed *compressedChunk) {
				if !send(compressed) {
					fc.logger.Error("send message failure before close channel, message lost")
				}
			}
			// flush chunk pending data if chunk not empty
			if !fc.chunk.IsEmpty() {
				// flush chunk pending data if chunk not empty
				compressed, err0 := fc.chunk.Compress()
				if err0 != nil {
					fc.logger.Error("compress chunk err when send last chunk data", logger.Error(err0))
				} else {
					sendLastMsg(compressed)
				}
			}
			// try to write pending data
			for compressed := range fc.ch {
				sendLastMsg(compressed)
			}
			return
		case <-fc.leaderChangedSignal:
			if stream != nil {
				fc.logger.Info("shard leader changed, need switch send stream",
					logger.String("oldTarget", fc.currentTarget.Indicator()),
					logger.String("database", fc.database))
				// if stream isn't nil, need close old stream first.
				if err = stream.Close(); err != nil {
					fc.logger.Error("close write stream err when leader changed", logger.Error(err))
				}
				stream = nil
			}
		case compressed := <-fc.ch:
			if send(compressed) {
				// if send ok, retry pending message
				if len(retryBuffers) > 0 {
					messages := retryBuffers
					retryBuffers = make([]*compressedChunk, 0)
					for _, msg := range messages {
						if !send(msg) {
							retry(msg)
						}
					}
				}
			} else {
				stream = nil
			}
		case <-ticker.C:
			// check
			fc.checkFlush()
		}
	}
}

// checkFlush checks if channel needs to flush data.
func (fc *familyChannel) checkFlush() {
	now := timeutil.Now()
	if now-fc.lastFlushTime.Load() >= fc.batchTimout.Milliseconds() {
		fc.lock4write.Lock()
		defer fc.lock4write.Unlock()

		if !fc.chunk.IsEmpty() {
			fc.flushChunk()
		}
		fc.lastFlushTime.Store(now)
	}
}

// Stop stops current write family shardChannel.
func (fc *familyChannel) Stop() {
	fc.cancel()
	close(fc.ch)

	activeWriteFamilies.WithTagValues(fc.database).Incr()
}

// flushChunk flushes the chunk data and appends data into queue
func (fc *familyChannel) flushChunk() {
	compressed, err := fc.chunk.Compress()
	if err != nil {
		fc.logger.Error("compress chunk err", logger.Error(err))
		return
	}
	if compressed == nil || len(*compressed) == 0 {
		return
	}
	select {
	case fc.ch <- compressed:
		pendingSend.WithTagValues(fc.database).Incr()
	case <-fc.ctx.Done():
		fc.logger.Warn("writer is canceled")
	}
}

// isExpire returns if current family is expired.
func (fc *familyChannel) isExpire(ahead, _ int64) bool {
	now := timeutil.Now()
	// add 15 minute buffer
	if ahead > 0 && fc.lastFlushTime.Load()+ahead+15*time.Minute.Milliseconds() > now {
		return false
	}
	return true
}

// FamilyTime returns the family time of current shardChannel.
func (fc *familyChannel) FamilyTime() int64 {
	return fc.familyTime
}
