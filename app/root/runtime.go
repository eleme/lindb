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

package root

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/lindb/lindb/app/root/api"
	depspkg "github.com/lindb/lindb/app/root/deps"
	"github.com/lindb/lindb/config"
	"github.com/lindb/lindb/constants"
	"github.com/lindb/lindb/coordinator/discovery"
	"github.com/lindb/lindb/coordinator/root"
	"github.com/lindb/lindb/internal/concurrent"
	"github.com/lindb/lindb/internal/linmetric"
	"github.com/lindb/lindb/internal/server"
	"github.com/lindb/lindb/metrics"
	"github.com/lindb/lindb/models"
	"github.com/lindb/lindb/pkg/hostutil"
	httppkg "github.com/lindb/lindb/pkg/http"
	"github.com/lindb/lindb/pkg/logger"
	"github.com/lindb/lindb/pkg/state"
	"github.com/lindb/lindb/pkg/timeutil"
	"github.com/lindb/lindb/query"
	"github.com/lindb/lindb/rpc"
	"github.com/lindb/lindb/series/tag"
)

// just for testing
var (
	getHostIP              = hostutil.GetHostIP
	hostName               = os.Hostname
	newTaskClientFactory   = rpc.NewTaskClientFactory
	newStateMachineFactory = root.NewStateMachineFactory
	newTaskManager         = query.NewTaskManager
	newRepositoryFactory   = state.NewRepositoryFactory
	newHTTPServer          = httppkg.NewServer
)

// deps represents all dependencies for root.
type deps struct {
	taskClientFct   rpc.TaskClientFactory
	connectionMgr   rpc.ConnectionManager
	repoFct         state.RepositoryFactory
	stateMachineFct discovery.StateMachineFactory
	stateMgr        root.StateManager
	taskMgr         query.TaskManager
}

// runtime represents root runtime dependency.
type runtime struct {
	version         string
	config          *config.Root
	state           server.State
	node            *models.StatelessNode
	globalKeyValues tag.Tags

	ctx    context.Context
	cancel context.CancelFunc

	deps *deps

	repo       state.Repository
	httpServer httppkg.Server

	logger *logger.Logger
}

// NewRootRuntime creates the root runtime.
func NewRootRuntime(version string, cfg *config.Root) server.Service {
	ctx, cancel := context.WithCancel(context.Background())
	return &runtime{
		version: version,
		config:  cfg,
		state:   server.New,
		ctx:     ctx,
		cancel:  cancel,
		logger:  logger.GetLogger("Root", "Runtime"),
	}
}

// Name returns the root service's name.
func (r *runtime) Name() string {
	return "root"
}

// Run runs root server.
func (r *runtime) Run() error {
	ip, err := getHostIP()
	if err != nil {
		r.state = server.Failed
		return fmt.Errorf("failed to get server ip address, error: %s", err)
	}
	hostName, err := hostName()
	if err != nil {
		r.logger.Error("failed to get host name", logger.Error(err))
		hostName = "unknown"
	}
	r.node = &models.StatelessNode{
		HostIP:     ip,
		HostName:   hostName,
		HTTPPort:   r.config.HTTP.Port,
		OnlineTime: timeutil.Now(),
		Version:    config.Version,
	}
	r.globalKeyValues = tag.Tags{
		{Key: []byte("node"), Value: []byte(r.node.Indicator())},
		{Key: []byte("role"), Value: []byte(constants.RootRole)},
	}
	r.logger.Info("starting root", logger.String("host", hostName), logger.String("ip", ip),
		logger.Uint16("http", r.node.HTTPPort))

	// build dependencies
	repoFct := newRepositoryFactory("root")
	taskClientFct := newTaskClientFactory(r.ctx, r.node, rpc.GetBrokerClientConnFactory())
	connectionMgr := rpc.NewConnectionManager(taskClientFct)
	stateMgr := root.NewStateManager(r.ctx, repoFct, connectionMgr)
	taskMgr := newTaskManager(
		concurrent.NewPool(
			"task-pool",
			r.config.Query.QueryConcurrency,
			r.config.Query.IdleTimeout.Duration(),
			metrics.NewConcurrentStatistics("root-query", linmetric.RootRegistry)),
		linmetric.RootRegistry)
	taskClientFct.SetTaskReceiver(taskMgr)
	r.deps = &deps{
		taskClientFct: taskClientFct,
		connectionMgr: connectionMgr,
		repoFct:       repoFct,
		stateMgr:      stateMgr,
		taskMgr:       taskMgr,
	}

	// start state repository
	if err = r.startStateRepo(); err != nil {
		r.logger.Error("failed to start state repo", logger.Error(err))
		r.state = server.Failed
		return err
	}
	discoveryFactory := discovery.NewFactory(r.repo)
	stateMachineFct := newStateMachineFactory(r.ctx, discoveryFactory, stateMgr)

	// finally, start all state machine
	if err := stateMachineFct.Start(); err != nil {
		return fmt.Errorf("start state machines error: %s", err)
	}
	r.deps.stateMachineFct = stateMachineFct
	// start http server
	r.startHTTPServer()

	r.state = server.Running
	return nil
}

// State returns current root server state.
func (r *runtime) State() server.State {
	return r.state
}

// Stop stops root server.
func (r *runtime) Stop() {
	r.logger.Info("stopping root server...")
	defer r.cancel()

	if r.deps.stateMachineFct != nil {
		r.logger.Info("stopping state machines...")
		r.deps.stateMachineFct.Stop()
		r.logger.Info("stopped state machines successfully")
	}
	if r.httpServer != nil {
		r.logger.Info("stopping http server...")
		if err := r.httpServer.Close(r.ctx); err != nil {
			r.logger.Error("shutdown http server error", logger.Error(err))
		} else {
			r.logger.Info("stopped http server successfully")
		}
	}

	r.state = server.Terminated
}

// startHTTPServer starts http server for api rpcHandler.
func (r *runtime) startHTTPServer() {
	if r.config.HTTP.Port <= 0 {
		r.logger.Info("http server is disabled as http-port is 0")
		return
	}

	r.httpServer = newHTTPServer(r.config.HTTP, true, linmetric.RootRegistry)
	// TODO: login api is not registered
	httpAPI := api.NewAPI(&depspkg.HTTPDeps{
		Ctx:          r.ctx,
		Cfg:          r.config,
		Node:         r.node,
		Repo:         r.repo,
		RepoFactory:  r.deps.repoFct,
		StateMgr:     r.deps.stateMgr,
		TransportMgr: query.NewTransportManager(r.deps.taskClientFct, nil, linmetric.RootRegistry), // root node no grpc server
		TaskMgr:      r.deps.taskMgr,
		QueryLimiter: concurrent.NewLimiter(
			r.ctx,
			r.config.Query.QueryConcurrency,
			r.config.Query.Timeout.Duration(),
			metrics.NewLimitStatistics("query", linmetric.RootRegistry),
		),
	})
	httpAPI.RegisterRouter(r.httpServer.GetAPIRouter())
	go func() {
		r.runHTTPServer()
	}()
}

// runHTTPServer runs http server.
func (r *runtime) runHTTPServer() {
	if err := r.httpServer.Run(); err != nil && err != http.ErrServerClosed {
		panic(fmt.Sprintf("start http server with error: %s", err))
	}
	r.logger.Info("http server stopped successfully")
}

// startStateRepo starts state repository.
func (r *runtime) startStateRepo() error {
	// set a sub namespace
	repo, err := r.deps.repoFct.CreateRootRepo(&r.config.Coordinator)
	if err != nil {
		return fmt.Errorf("start root state repository error:%s", err)
	}
	r.repo = repo
	r.logger.Info("start root state repository successfully")
	return nil
}
