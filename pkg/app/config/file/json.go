package file

import (
	"github.com/ch-rollup/ch-rollup/pkg/app/config"
	"github.com/ch-rollup/ch-rollup/pkg/types"
	"github.com/ch-rollup/ch-rollup/pkg/utils/json/duration"
	sliceUtils "github.com/ch-rollup/ch-rollup/pkg/utils/slice"
)

type clickHouseJSON struct {
	Address     string `json:"address"`
	UserName    string `json:"user_name"`
	ClusterName string `json:"cluster_name"`
}

type configJSON struct {
	ClickHouse clickHouseJSON `json:"clickhouse"`
	Tasks      []taskJSON     `json:"tasks"`
}

type taskJSON struct {
	Database       string              `json:"database"`
	Table          string              `json:"table"`
	PartitionKey   duration.Duration   `json:"partition_key"`
	RollUpSettings []rollUpSettingJSON `json:"roll_up_settings"`
	ColumnSettings []columnSettingJSON `json:"column_settings"`
}

type rollUpSettingJSON struct {
	NextRunAfter   duration.Duration   `json:"next_run_after"`
	Interval       duration.Duration   `json:"interval"`
	ColumnSettings []columnSettingJSON `json:"column_settings"`
}

type columnSettingJSON struct {
	Name                  string `json:"name"`
	AutoResolveExpression bool   `json:"auto_resolve_expression"`
	IsRollUpTime          bool   `json:"is_roll_up_time"`
	Expression            string `json:"expression"`
}

func bindConfigFromJSON(cfg configJSON) config.Config {
	return config.Config{
		ClickHouse: bindClickHouseFromJSON(cfg.ClickHouse),
		Tasks:      sliceUtils.ConvertFunc(cfg.Tasks, bindTaskFromJSON),
	}
}

func bindClickHouseFromJSON(clickHouse clickHouseJSON) config.ClickHouse {
	return config.ClickHouse{
		Address:     clickHouse.Address,
		UserName:    clickHouse.UserName,
		ClusterName: clickHouse.ClusterName,
	}
}

func bindTaskFromJSON(task taskJSON) types.Task {
	return types.Task{
		Database:       task.Database,
		Table:          task.Table,
		PartitionKey:   task.PartitionKey.Duration,
		RollUpSettings: sliceUtils.ConvertFunc(task.RollUpSettings, bindRollUpSettingFromJSON),
		ColumnSettings: sliceUtils.ConvertFunc(task.ColumnSettings, bindColumnSettingsFromJSON),
	}
}

func bindRollUpSettingFromJSON(rollUpSetting rollUpSettingJSON) types.RollUpSetting {
	return types.RollUpSetting{
		NextRunAfter:   rollUpSetting.NextRunAfter.Duration,
		Interval:       rollUpSetting.Interval.Duration,
		ColumnSettings: sliceUtils.ConvertFunc(rollUpSetting.ColumnSettings, bindColumnSettingsFromJSON),
	}
}

func bindColumnSettingsFromJSON(columnSetting columnSettingJSON) types.ColumnSetting {
	return types.ColumnSetting{
		Name:                  columnSetting.Name,
		AutoResolveExpression: columnSetting.AutoResolveExpression,
		IsRollUpTime:          columnSetting.IsRollUpTime,
		Expression:            columnSetting.Expression,
	}
}
