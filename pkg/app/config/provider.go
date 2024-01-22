package config

type WatchFunc func(c Config)

type Provider interface {
	GetConfig() Config
	AddWatcher(f WatchFunc)
}
