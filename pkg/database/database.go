package database

import (
	"context"

	"github.com/ch-rollup/ch-rollup/pkg/database/cluster"
)

// Cluster ...
type Cluster interface {
	ExecOnCluster(ctx context.Context, query string, args ...string) error
	GetShards() []cluster.Shard
	Close() error
}

// Database ...
type Database struct {
	cluster Cluster
}

// New returns new Database.
func New(ctx context.Context, cluster Cluster) (*Database, error) {
	db := Database{
		cluster: cluster,
	}

	if err := db.setUp(ctx); err != nil {
		return nil, err
	}

	return &db, nil
}

// Close Database.
func (db *Database) Close() error {
	return db.cluster.Close()
}
