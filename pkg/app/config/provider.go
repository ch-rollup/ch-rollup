package config

// WatchFunc ...
type WatchFunc func(c Config)

// Provider is an interface for Config Provider.
type Provider interface {
	// Get Config.
	Get() Config
	// AddWatcher for getting callbacks from Provider.
	AddWatcher(f WatchFunc)
}
