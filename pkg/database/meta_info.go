package database

import (
	"context"
	"time"

	"github.com/ch-rollup/ch-rollup/pkg/database/cluster"
	timeUtils "github.com/ch-rollup/ch-rollup/pkg/utils/time"
)

type rollUpMetaInfo struct {
	Database     string
	Table        string
	NextRunAfter time.Duration
	Interval     time.Duration
	RollUpsAt    time.Time
}

type rollUpMetaInfoKey struct {
	Database     string
	Table        string
	NextRunAfter time.Duration
	Interval     time.Duration
}

func getLatestRollUpByKeyOnShard(ctx context.Context, shard cluster.Shard, key rollUpMetaInfoKey) (time.Time, error) {
	var rollUpsAt time.Time

	err := shard.QueryRow(ctx,
		`
		SELECT
    		max(roll_ups_at)
		FROM
    		rollup_meta_info
		WHERE 
		    database = $1 AND table = $2 AND next_run_after_sec = $3 AND interval_sec = $4 
		GROUP BY 
		    database, table, next_run_after_sec, interval_sec;
	`, key.Database, key.Table, timeUtils.SecondsFromDuration(key.NextRunAfter), timeUtils.SecondsFromDuration(key.Interval)).Scan(&rollUpsAt)
	if err != nil {
		return time.Time{}, err
	}

	return rollUpsAt, nil
}

func addRollUpMetaInfoOnShard(ctx context.Context, shard cluster.Shard, metaInfo rollUpMetaInfo) error {
	return shard.Exec(
		ctx,
		`
			INSERT INTO  
				rollup_meta_info(database, table, next_run_after_sec, interval_sec, roll_ups_at)
			VALUES 
			    (?, ?, ?, ?, ?)
	`, metaInfo.Database, metaInfo.Table, timeUtils.SecondsFromDuration(metaInfo.NextRunAfter), timeUtils.SecondsFromDuration(metaInfo.Interval), metaInfo.RollUpsAt)
}
