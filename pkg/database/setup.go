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
				after_sec UInt64,
				duration_sec UInt64,
				roll_ups_at DateTime
			) ENGINE = MergeTree() ORDER BY (database, table, after_sec, duration_sec, roll_ups_at);
	`
)

func (db *Database) setUp(ctx context.Context) error {
	if err := db.cluster.ExecOnCluster(ctx, rollUpTableDefinition); err != nil {
		return fmt.Errorf("failed to create rollup meta info table: %w", err)
	}

	return nil
}
