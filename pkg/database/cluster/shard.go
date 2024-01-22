package cluster

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Shard struct {
	name string
	conn clickhouse.Conn
}

func (c *Shard) Exec(ctx context.Context, query string, args ...any) error {
	return c.conn.Exec(ctx, query, args...)
}

func (c *Shard) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

func (c *Shard) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return c.conn.QueryRow(ctx, query, args...)
}

func NewShard(name string, conn clickhouse.Conn) Shard {
	return Shard{
		name: name,
		conn: conn,
	}
}
