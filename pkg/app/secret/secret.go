// Package secret defines Secrets and it Provider.
package secret

// Secrets ...
type Secrets struct {
	ClickHousePassword string
}

// Provider is an interface for Secrets Provider.
type Provider interface {
	// Get Secrets.
	Get() Secrets
}
