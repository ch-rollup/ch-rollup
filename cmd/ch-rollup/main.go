// Package main is a ch-rollup start point.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	"github.com/ch-rollup/ch-rollup/internal/buildinfo"
	"github.com/ch-rollup/ch-rollup/pkg/app"
	file_config "github.com/ch-rollup/ch-rollup/pkg/app/config/file"
	env_secrets "github.com/ch-rollup/ch-rollup/pkg/app/secret/env"
)

var (
	showVersion = flag.Bool("version", false, "Print version")
	configPath  = flag.String("config-path", "./config.json", "Path to config")
)

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Println(buildinfo.Get().String())
		return
	}

	logger := slog.New(
		slog.NewJSONHandler(os.Stdout, nil),
	)

	if err := run(*configPath, logger); err != nil {
		logger.Error("failed to run app", slog.String("error", err.Error()))
	}
}

func run(configPath string, logger *slog.Logger) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	envSecretsProvider, err := env_secrets.New()
	if err != nil {
		return err
	}

	fileCfgProvider, err := file_config.New(configPath)
	if err != nil {
		return err
	}

	return app.Run(
		ctx,
		app.RunOptions{
			SecretProvider: envSecretsProvider,
			ConfigProvider: fileCfgProvider,
			Logger: slogToAppLoggerAdapter(
				logger.WithGroup("app"),
			),
		},
	)
}
