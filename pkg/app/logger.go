package app

// Logger for app.
type Logger interface {
	Info(msg string)
	Error(msg string)
}
