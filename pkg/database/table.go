package database

import (
	"context"
	"fmt"
	"github.com/ch-rollup/ch-rollup/pkg/database/cluster"
)

func createTableAsOnShard(ctx context.Context, shard cluster.Shard, database, srcTable, dstTable string) error {
	if err := shard.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.%s AS %s.%s", database, dstTable, database, srcTable)); err != nil {
		return fmt.Errorf("failed to create table %s as %s in %s: %w", dstTable, srcTable, database, err)
	}

	return nil
}

func dropTableOnShard(ctx context.Context, shard cluster.Shard, database, table string) error {
	if err := shard.Exec(ctx, fmt.Sprintf("DROP TABLE %s.%s", database, table)); err != nil {
		return fmt.Errorf("failed to drop table %s in %s: %w", table, database, err)
	}

	return nil
}
