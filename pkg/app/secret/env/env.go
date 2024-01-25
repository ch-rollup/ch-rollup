// Package env implements Env secret.Provider.
package env

import (
	"fmt"
	"os"

	"github.com/ch-rollup/ch-rollup/pkg/app/secret"
)

const (
	clickHousePasswordKey = "CLICKHOUSE_PASSWORD"
)

// Env implementation of secret.Provider.
type Env struct {
	secrets secret.Secrets
}

// New returns new Env secret.Provider.
func New() (*Env, error) {
	clickHousePassword, ok := os.LookupEnv(clickHousePasswordKey)
	if !ok {
		return nil, fmt.Errorf("required key %s doesen't present in ENV", clickHousePasswordKey)
	}

	return &Env{
		secrets: secret.Secrets{
			ClickHousePassword: clickHousePassword,
		},
	}, nil
}

// Get secret.Secrets.
func (e *Env) Get() secret.Secrets {
	return e.secrets
}
