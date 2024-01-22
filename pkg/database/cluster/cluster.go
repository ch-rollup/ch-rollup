package cluster

import (
	"context"
	"fmt"

	"go.uber.org/multierr"
)

type Cluster struct {
	shards []Shard
}

func NewCluster(shards []Shard) *Cluster {
	return &Cluster{
		shards: shards,
	}
}

func (c *Cluster) GetShards() []Shard {
	return c.shards
}

func (c *Cluster) ExecOnCluster(ctx context.Context, query string, args ...string) error {
	for _, shard := range c.shards {
		if err := shard.conn.Exec(ctx, query, args); err != nil {
			return fmt.Errorf("failed to exec query on %s: %w", shard.name, err)
		}
	}

	return nil
}

func (c *Cluster) Close() (err error) {
	for _, shard := range c.shards {
		err = multierr.Append(err, shard.conn.Close())
	}

	return err
}
