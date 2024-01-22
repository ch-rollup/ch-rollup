package database

import (
	"context"
	"fmt"
	"github.com/ch-rollup/ch-rollup/pkg/database/cluster"
)

func getPartitionsOnShard(ctx context.Context, shard cluster.Shard, database, table string) ([]string, error) {
	rows, err := shard.Query(ctx, `
		SELECT 
		    partition
		FROM 
		    system.parts
		WHERE
		    database = $1 AND table = $2 AND active = 1
	`, database, table)
	if err != nil {
		return nil, err
	}

	var result []string

	defer rows.Close()

	for rows.Next() {
		var partition string

		if err = rows.Scan(&partition); err != nil {
			return nil, err
		}

		result = append(result, partition)
	}

	return result, nil
}

func replacePartitionsOnShard(ctx context.Context, shard cluster.Shard, database, from, to string, partitions []string) error {
	// TODO: generate multistatement query.
	for _, partition := range partitions {
		err := shard.Exec(ctx,
			fmt.Sprintf(`
			ALTER TABLE 
		   		%s.%s 
			REPLACE PARTITION 
		    	%s
			FROM %s.%s
		`, database, to, partition, database, from),
		)
		if err != nil {
			return err
		}
	}

	return nil
}
