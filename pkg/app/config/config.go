package config

import (
	"errors"
	"fmt"

	"github.com/ch-rollup/ch-rollup/pkg/types"
)

type ClickHouse struct {
	Address     string
	UserName    string
	ClusterName string
}

type Config struct {
	ClickHouse ClickHouse
	Tasks      []types.Task
}

var ErrBadConfig = errors.New("bad config")

func (c Config) Validate() error {
	if c.ClickHouse.Address == "" {
		return fmt.Errorf("address must be not empty: %w", ErrBadConfig)
	}

	if c.ClickHouse.UserName == "" {
		return fmt.Errorf("user name must be not empty: %w", ErrBadConfig)
	}

	for _, task := range c.Tasks {
		if err := task.Validate(); err != nil {
			return err
		}
	}

	return nil
}
