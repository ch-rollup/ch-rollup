package database

import (
	"context"
	"time"

	"github.com/ch-rollup/ch-rollup/pkg/database/cluster"
	timeUtils "github.com/ch-rollup/ch-rollup/pkg/utils/time"
)

type rollUpMetaInfo struct {
	DataBase  string
	Table     string
	After     time.Duration
	Duration  time.Duration
	RollUpsAt time.Time
}

type rollUpMetaInfoKey struct {
	DataBase string
	Table    string
	After    time.Duration
	Duration time.Duration
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
		    database = $1 AND table = $2 AND after_sec = $3 AND duration_sec = $4 
		GROUP BY 
		    database, table, after_sec, duration_sec;
	`, key.DataBase, key.Table, timeUtils.SecondsFromDuration(key.After), timeUtils.SecondsFromDuration(key.Duration)).Scan(&rollUpsAt)
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
				rollup_meta_info(database, table, after_sec, duration_sec, roll_ups_at)
			VALUES 
			    (?, ?, ?, ?, ?)
	`, metaInfo.DataBase, metaInfo.Table, timeUtils.SecondsFromDuration(metaInfo.After), timeUtils.SecondsFromDuration(metaInfo.Duration), metaInfo.RollUpsAt)
}
