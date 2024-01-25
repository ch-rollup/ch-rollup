package cluster

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Shard ...
type Shard struct {
	name string
	conn clickhouse.Conn
}

// Exec ...
func (c *Shard) Exec(ctx context.Context, query string, args ...any) error {
	return c.conn.Exec(ctx, query, args...)
}

// Query ...
func (c *Shard) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

// QueryRow ...
func (c *Shard) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return c.conn.QueryRow(ctx, query, args...)
}

// NewShard returns new Shard.
func NewShard(name string, conn clickhouse.Conn) Shard {
	return Shard{
		name: name,
		conn: conn,
	}
}
