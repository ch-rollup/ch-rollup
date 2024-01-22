package database

import (
	"context"
	"github.com/ch-rollup/ch-rollup/pkg/database/cluster"
)

type Cluster interface {
	ExecOnCluster(ctx context.Context, query string, args ...string) error
	GetShards() []cluster.Shard
	Close() error
}

type Database struct {
	cluster Cluster
}

func New(ctx context.Context, cluster Cluster) (*Database, error) {
	db := Database{
		cluster: cluster,
	}

	if err := db.setUp(ctx); err != nil {
		return nil, err
	}

	return &db, nil
}

func (db *Database) Close() error {
	return db.cluster.Close()
}
