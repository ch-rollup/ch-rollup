package secret

type Secrets struct {
	ClickHousePassword string
}

type Provider interface {
	GetSecrets() Secrets
}
