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

	// TODO: rename this field and underlying type coz they're misleads the end user.
	Tasks []types.Task
}

// ErrBadConfig ...
var ErrBadConfig = errors.New("bad config")

// Validate Config.
func (c Config) Validate() error {
	if c.ClickHouse.Address == "" {
		return fmt.Errorf("address must not be empty: %w", ErrBadConfig)
	}

	if c.ClickHouse.UserName == "" {
		return fmt.Errorf("user name must not be empty: %w", ErrBadConfig)
	}

	for _, task := range c.Tasks {
		if err := task.Validate(); err != nil {
			return err
		}
	}

	return nil
}
