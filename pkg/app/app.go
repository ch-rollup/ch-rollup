// Package app implements rollup as app.
package app

import (
	"context"
	"errors"

	"github.com/ch-rollup/ch-rollup/pkg/app/config"
	"github.com/ch-rollup/ch-rollup/pkg/app/secret"
	"github.com/ch-rollup/ch-rollup/pkg/database"
	"github.com/ch-rollup/ch-rollup/pkg/database/cluster"
	"github.com/ch-rollup/ch-rollup/pkg/scheduler"
)

// RunOptions is an options for Run
type RunOptions struct {
	SecretProvider secret.Provider
	ConfigProvider config.Provider
	Logger         Logger
}

// Run app with provided RunOptions
func Run(ctx context.Context, opts RunOptions) error {
	logger := opts.Logger

	secrets := opts.SecretProvider.Get()
	cfg := opts.ConfigProvider.Get()

	chCluster, err := cluster.Connect(ctx, cluster.ConnectOptions{
		Address:     cfg.ClickHouse.Address,
		Username:    cfg.ClickHouse.UserName,
		Password:    secrets.ClickHousePassword,
		ClusterName: cfg.ClickHouse.ClusterName,
	})
	if err != nil {
		return err
	}

	db, err := database.New(ctx, chCluster)
	if err != nil {
		return err
	}

	s, err := scheduler.New(ctx, db, cfg.Tasks)
	if err != nil {
		return err
	}

	events, err := s.Run(ctx)
	if err != nil {
		return err
	}

	logger.Info("scheduler was started successfully")

	for event := range events {
		eventString := event.String()

		if event.Error != nil && !errors.Is(err, context.Canceled) {
			logger.Error(eventString)
			continue
		}

		logger.Info(eventString)
	}

	// after events channel closed - ctx is done and we can gracefully shut down app.

	if err = db.Close(); err != nil {
		return err
	}

	return nil
}
