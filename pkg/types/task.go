package types

import (
	"fmt"
	"time"
)

type Tasks []Task

func (t Tasks) Validate() error {
	for _, task := range t {
		if err := task.Validate(); err != nil {
			return err
		}
	}

	return nil
}

type Task struct {
	DataBase       string
	Table          string
	PartitionKey   time.Duration
	RollUpSettings []RollUpSetting
	ColumnSettings []ColumnSetting
}

type RollUpSetting struct {
	RollUpAfter    time.Duration
	RollUpDuration time.Duration
	ColumnSettings []ColumnSetting
}

type ColumnSetting struct {
	Name                  string
	AutoResolveExpression bool // TODO: implement auto resolve expression
	IsRollUpTime          bool
	Expression            string
}

func (t *Task) Validate() error {
	if t.Table == "" {
		return fmt.Errorf("field Table must be not empty")
	}

	if t.DataBase == "" {
		return fmt.Errorf("field DataBase must be not empty")
	}

	if t.PartitionKey <= 0 {
		return fmt.Errorf("field PartitionKey must be not empty")
	}

	var rollUpTimeColumnName string

	for _, columnSetting := range t.ColumnSettings {
		if err := columnSetting.Validate(); err != nil {
			return err
		}

		if columnSetting.IsRollUpTime {
			if rollUpTimeColumnName != "" {
				return fmt.Errorf("there should only be one rollUp time column")
			}

			rollUpTimeColumnName = columnSetting.Name
		}
	}

	if rollUpTimeColumnName == "" {
		return fmt.Errorf("rollup time column must be defined")
	}

	for _, rollUpSetting := range t.RollUpSettings {
		if err := rollUpSetting.Validate(rollUpTimeColumnName); err != nil {
			return err
		}
	}

	return nil
}

func (rs *RollUpSetting) Validate(rollUpTimeColumnName string) error {
	if rs.RollUpAfter <= 0 {
		return fmt.Errorf("field RollUpAfter must be not empty")
	}

	if rs.RollUpDuration <= 0 {
		return fmt.Errorf("field RollUpDuration must be not empty")
	}

	for _, columnSetting := range rs.ColumnSettings {
		if err := columnSetting.Validate(); err != nil {
			return err
		}

		if columnSetting.IsRollUpTime || columnSetting.Name == rollUpTimeColumnName {
			return fmt.Errorf("roll up time collumn can be defined only in global settings")
		}
	}

	return nil
}

func (cs *ColumnSetting) Validate() error {
	if cs.Name == "" {
		return fmt.Errorf("field Name must be not empty")
	}

	return nil
}
