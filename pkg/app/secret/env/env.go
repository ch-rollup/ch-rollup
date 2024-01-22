package env

import (
	"fmt"
	"os"

	"github.com/ch-rollup/ch-rollup/pkg/app/secret"
)

const (
	clickHousePasswordKey = "CLICKHOUSE_PASSWORD"
)

type Env struct {
	secrets secret.Secrets
}

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

func (e *Env) GetSecrets() secret.Secrets {
	return e.secrets
}
