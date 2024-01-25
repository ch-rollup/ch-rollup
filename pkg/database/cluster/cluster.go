// Package cluster implements ClickHouse Cluster layer.
package cluster

import (
	"context"
	"fmt"

	"go.uber.org/multierr"
)

// Cluster of ClickHouse.
type Cluster struct {
	shards []Shard
}

// NewCluster returns new Cluster.
func NewCluster(shards []Shard) *Cluster {
	return &Cluster{
		shards: shards,
	}
}

// GetShards returns shards.
func (c *Cluster) GetShards() []Shard {
	return c.shards
}

// ExecOnCluster executes query on all shards.
func (c *Cluster) ExecOnCluster(ctx context.Context, query string, args ...string) error {
	for _, shard := range c.shards {
		if err := shard.conn.Exec(ctx, query, args); err != nil {
			return fmt.Errorf("failed to exec query on %s: %w", shard.name, err)
		}
	}

	return nil
}

// Close Cluster.
func (c *Cluster) Close() (err error) {
	for _, shard := range c.shards {
		err = multierr.Append(err, shard.conn.Close())
	}

	return err
}
