// Package config defined Config and it Provider.
package config

import (
	"errors"
	"fmt"

	"github.com/ch-rollup/ch-rollup/pkg/types"
)

// ClickHouse ...
type ClickHouse struct {
	Address     string
	UserName    string
	ClusterName string
}

// Config ...
type Config struct {
	ClickHouse ClickHouse
	Tasks      []types.Task
}

// ErrBadConfig ...
var ErrBadConfig = errors.New("bad config")

// Validate Config.
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
