package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/multierr"

	"github.com/ch-rollup/ch-rollup/pkg/database/cluster"
	"github.com/ch-rollup/ch-rollup/pkg/types"
	timeUtils "github.com/ch-rollup/ch-rollup/pkg/utils/time"
)

// RollUpOptions ...
type RollUpOptions struct {
	DataBase     string
	Table        string
	TempTable    string
	PartitionKey time.Duration
	Columns      []types.ColumnSetting
	Duration     time.Duration
	After        time.Duration
	Interval     time.Duration
}

var (
	// ErrBadRollUpOptions ...
	ErrBadRollUpOptions = errors.New("bad ErrBadRollUpOptions")
)

// Validate RollUpOptions.
func (opts RollUpOptions) Validate() error {
	if opts.DataBase == "" {
		return fmt.Errorf("data base must be not empty: %w", ErrBadRollUpOptions)
	}

	if opts.Table == "" {
		return fmt.Errorf("table must be not empty: %w", ErrBadRollUpOptions)
	}

	if opts.TempTable == "" {
		return fmt.Errorf("temp table must be not empty: %w", ErrBadRollUpOptions)
	}

	if opts.PartitionKey <= 0 {
		return fmt.Errorf("PartitionKey must be not empty: %w", ErrBadRollUpOptions)
	}

	if opts.Duration <= 0 {
		return fmt.Errorf("duration must be not empty: %w", ErrBadRollUpOptions)
	}

	if opts.After <= 0 {
		return fmt.Errorf("after must be not empty: %w", ErrBadRollUpOptions)
	}

	if opts.Interval <= 0 {
		return fmt.Errorf("interval must be not empty: %w", ErrBadRollUpOptions)
	}

	for _, column := range opts.Columns {
		if err := column.Validate(); err != nil {
			return err
		}
	}

	if timeColumnName := getTimeColumnName(opts.Columns); timeColumnName == "" {
		return fmt.Errorf("you must specify time column: %w", ErrBadRollUpOptions)
	}

	return nil
}

// RollUp Database with RollUpOptions.
func (db *Database) RollUp(ctx context.Context, opts RollUpOptions) error {
	if err := opts.Validate(); err != nil {
		return fmt.Errorf("fail to validate RollUpOptions: %w", err)
	}

	for _, shard := range db.cluster.GetShards() {
		err := RollUpShard(ctx, shard, opts)
		if err != nil {
			return err
		}
	}

	return nil
}

// RollUpShard cluster.Shard with RollUpOptions.
func RollUpShard(ctx context.Context, shard cluster.Shard, opts RollUpOptions) (err error) {
	if err = opts.Validate(); err != nil {
		return fmt.Errorf("fail to validate RollUpOptions: %w", err)
	}

	latestRollUp, err := getLatestRollUpByKeyOnShard(ctx, shard, rollUpMetaInfoKey{
		DataBase: opts.DataBase,
		Table:    opts.Table,
		After:    opts.After,
		Duration: opts.Duration,
	})
	if err != nil {
		// TODO: check error
		if err = createRollUpMetaInfo(ctx, shard, time.Now().Truncate(opts.PartitionKey), opts); err != nil {
			return err
		}

		return nil
	}

	rollUpTo := time.Now().Add(-opts.After).Truncate(opts.PartitionKey)
	if rollUpTo.Before(latestRollUp) {
		// TODO: add error here
		return nil
	}

	if err = createTableAsOnShard(ctx, shard, opts.DataBase, opts.Table, opts.TempTable); err != nil {
		return err
	}

	defer func() {
		if dropErr := dropTableOnShard(ctx, shard, opts.DataBase, opts.TempTable); dropErr != nil {
			err = multierr.Append(err, dropErr)
		}
	}()

	// need from (latestRollUp) / to (rollUpTo) / interval (opts)
	query := generateRollUpStatement(generateRollUpStatementOptions{
		DataBase:  opts.DataBase,
		FromTable: opts.Table,
		ToTable:   opts.TempTable,
		Duration:  opts.Duration,
		Columns:   opts.Columns,
	})

	intervals := timeUtils.SplitTimeRangeByInterval(
		timeUtils.Range{
			From: latestRollUp,
			To:   rollUpTo,
		},
		opts.Interval,
	)

	for _, interval := range intervals {
		if err = shard.Exec(ctx, query, interval.From, interval.To); err != nil {
			return err
		}
	}

	partitions, err := getPartitionsOnShard(ctx, shard, opts.DataBase, opts.TempTable)
	if err != nil {
		return fmt.Errorf("fail to get %s.%s partitions: %w", opts.DataBase, opts.Table, err)
	}

	if err = replacePartitionsOnShard(ctx, shard, opts.DataBase, opts.TempTable, opts.Table, partitions); err != nil {
		return fmt.Errorf("fail to replace partitions from %s.%s to %s.%s: %w", opts.DataBase, opts.TempTable, opts.DataBase, opts.Table, err)
	}

	return createRollUpMetaInfo(ctx, shard, rollUpTo, opts)
}

func createRollUpMetaInfo(ctx context.Context, shard cluster.Shard, rollUpsAt time.Time, opts RollUpOptions) error {
	return addRollUpMetaInfoOnShard(ctx, shard, rollUpMetaInfo{
		DataBase:  opts.DataBase,
		Table:     opts.Table,
		After:     opts.After,
		Duration:  opts.Duration,
		RollUpsAt: rollUpsAt,
	})
}
