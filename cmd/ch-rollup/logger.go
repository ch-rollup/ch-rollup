package main

import (
	"log/slog"
)

type appLogger struct {
	logger *slog.Logger
}

func (l appLogger) Info(msg string) {
	l.logger.Info(msg)
}

func (l appLogger) Error(msg string) {
	l.logger.Error(msg)
}

func slogToAppLoggerAdapter(slog *slog.Logger) appLogger {
	return appLogger{
		logger: slog,
	}
}
