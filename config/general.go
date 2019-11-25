package config

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/lindb/lindb/pkg/ltoml"
)

// RepoState represents state repository config
type RepoState struct {
	Namespace   string         `toml:"namespace" json:"namespace"`
	Endpoints   []string       `toml:"endpoints" json:"endpoints"`
	DialTimeout ltoml.Duration `toml:"dial-timeout" json:"dialTimeout"`
}

// TOML returns RepoState's toml config string
func (rs *RepoState) TOML() string {
	coordinatorEndpoints, _ := json.Marshal(rs.Endpoints)
	return fmt.Sprintf(`
    ## Coordinator coordinates reads/writes operations between different nodes
    ## namespace organizes etcd keys into a isolated complete keyspaces for coordinator
	namespace = "%s"
    ## endpoints config list of ETCD cluster
    endpoints = %s
    ## ETCD client will fail at this interval when connecting etcd server without response
    dial-timeout = "%s"`,
		rs.Namespace,
		coordinatorEndpoints,
		rs.DialTimeout.String(),
	)
}

// GRPC represents grpc server config
type GRPC struct {
	Port uint16         `toml:"port"`
	TTL  ltoml.Duration `toml:"ttl"`
}

func (g *GRPC) TOML() string {
	return fmt.Sprintf(`
    port = %d
    ttl = "%s"`,
		g.Port,
		g.TTL.String(),
	)
}

// StorageCluster represents config of storage cluster
type StorageCluster struct {
	Name   string    `json:"name"`
	Config RepoState `json:"config"`
}

// Query represents query rpc config
type Query struct {
	MaxWorkers  int            `toml:"max-workers"`
	IdleTimeout ltoml.Duration `toml:"idle-timeout"`
	Timeout     ltoml.Duration `toml:"timeout"`
}

func (q *Query) TOML() string {
	return fmt.Sprintf(`
    ## max concurrentcy number of workers in the executor pool,
    ## each worker is only responsible for execting querying task,
    ## and idle workers will be recycled.
    max-workers = %d

    ## idle worker will be canceled in this duration
	idle-timeout = "%s"

    ## maximum timeout threshold for the task performed
    timeout = "%s"`,
		q.MaxWorkers,
		q.IdleTimeout,
		q.Timeout,
	)
}

func NewDefaultQuery() *Query {
	return &Query{
		MaxWorkers:  30,
		IdleTimeout: ltoml.Duration(5 * time.Second),
		Timeout:     ltoml.Duration(30 * time.Second),
	}
}
