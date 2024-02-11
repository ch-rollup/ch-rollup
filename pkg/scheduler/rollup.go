package scheduler

import (
	"context"
	"time"

	"golang.org/x/exp/maps"

	"github.com/ch-rollup/ch-rollup/pkg/database"
	"github.com/ch-rollup/ch-rollup/pkg/types"
)

const (
	tempTablePrefix = "_temp"
)

func (s *Scheduler) rollUp(ctx context.Context) error {
	for _, task := range s.getTasks() {
		for _, rollUpSetting := range task.RollUpSettings {
			err := s.db.RollUp(ctx, database.RollUpOptions{
				Database:     task.Database,
				Table:        task.Table,
				TempTable:    task.Table + tempTablePrefix,
				PartitionKey: task.PartitionKey,
				Columns:      prepareRollUpColumns(task.ColumnSettings, rollUpSetting.ColumnSettings),
				Interval:     rollUpSetting.Interval,
				NextRunAfter: rollUpSetting.NextRunAfter,
				CopyInterval: time.Hour, // TODO: add interval settings
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func prepareRollUpColumns(globalColumnSettings, currentColumnSettings []types.ColumnSetting) []types.ColumnSetting {
	result := make(map[string]types.ColumnSetting, len(globalColumnSettings)+len(currentColumnSettings))

	for _, columnSettings := range globalColumnSettings {
		result[columnSettings.Name] = columnSettings
	}

	for _, columnSettings := range currentColumnSettings {
		result[columnSettings.Name] = columnSettings
	}

	return maps.Values(result)
}
