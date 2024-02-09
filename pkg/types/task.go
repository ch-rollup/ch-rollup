// Package types is a general models for ch-rollup.
package types

import (
	"fmt"
	"time"
)

// Tasks ...
type Tasks []Task

// Validate Tasks.
func (t Tasks) Validate() error {
	for _, task := range t {
		if err := task.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// Task ...
type Task struct {
	Database       string
	Table          string
	PartitionKey   time.Duration
	RollUpSettings []RollUpSetting
	ColumnSettings []ColumnSetting
}

// RollUpSetting ...
type RollUpSetting struct {
	RollUpAfter    time.Duration
	RollUpDuration time.Duration
	ColumnSettings []ColumnSetting
}

// ColumnSetting ...
type ColumnSetting struct {
	Name                  string
	AutoResolveExpression bool // TODO: implement auto resolve expression
	IsRollUpTime          bool
	Expression            string
}

// Validate Task.
func (t *Task) Validate() error {
	if t.Table == "" {
		return fmt.Errorf("table must not be empty")
	}

	if t.Database == "" {
		return fmt.Errorf("database must not be empty")
	}

	if t.PartitionKey <= 0 {
		return fmt.Errorf("partitionKey must not be empty")
	}

	var rollUpTimeColumnName string

	for _, columnSetting := range t.ColumnSettings {
		if err := columnSetting.Validate(); err != nil {
			return err
		}

		if columnSetting.IsRollUpTime {
			if rollUpTimeColumnName != "" {
				return fmt.Errorf("only one rollUpTime column allowed")
			}

			rollUpTimeColumnName = columnSetting.Name
		}
	}

	if rollUpTimeColumnName == "" {
		return fmt.Errorf("rollupTimeColumn must not be empty")
	}

	for _, rollUpSetting := range t.RollUpSettings {
		if err := rollUpSetting.Validate(rollUpTimeColumnName); err != nil {
			return err
		}
	}

	return nil
}

// Validate RollUpSetting.
func (rs *RollUpSetting) Validate(rollUpTimeColumnName string) error {
	if rs.RollUpAfter <= 0 {
		return fmt.Errorf("rollUpAfter must not be empty")
	}

	if rs.RollUpDuration <= 0 {
		return fmt.Errorf("rollUpDuration must not be empty")
	}

	for _, columnSetting := range rs.ColumnSettings {
		if err := columnSetting.Validate(); err != nil {
			return err
		}

		if columnSetting.IsRollUpTime || columnSetting.Name == rollUpTimeColumnName {
			return fmt.Errorf("rollUpTime column can be defined only in global settings")
		}
	}

	return nil
}

// Validate ColumnSetting.
func (cs *ColumnSetting) Validate() error {
	if cs.Name == "" {
		return fmt.Errorf("name must not be empty")
	}

	return nil
}
