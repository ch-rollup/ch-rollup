package database

import (
	"context"
	"fmt"
)

const (
	rollUpTableDefinition = `
			CREATE TABLE IF NOT EXISTS rollup_meta_info(
				database String,
				table String,
				next_run_after_sec UInt64,
				interval_sec UInt64,
				roll_ups_at DateTime
			) ENGINE = MergeTree() ORDER BY (database, table, next_run_after_sec, interval_sec, roll_ups_at);
	`
)

func (db *Database) setUp(ctx context.Context) error {
	if err := db.cluster.ExecOnCluster(ctx, rollUpTableDefinition); err != nil {
		return fmt.Errorf("failed to create rollup meta info table: %w", err)
	}

	return nil
}
