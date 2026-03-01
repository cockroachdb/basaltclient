package basaltclient

import "log"

// Logger is the logging interface accepted by basaltclient and basaltfs.
// Implementations must be safe for concurrent use.
type Logger interface {
	Infof(format string, args ...any)
	Errorf(format string, args ...any)
}

// NopLogger discards all log output.
var NopLogger Logger = nopLogger{}

type nopLogger struct{}

func (nopLogger) Infof(string, ...any)  {}
func (nopLogger) Errorf(string, ...any) {}

// DefaultLogger logs to the standard log package.
var DefaultLogger Logger = defaultLogger{}

type defaultLogger struct{}

func (defaultLogger) Infof(format string, args ...any) {
	log.Printf(format, args...)
}

func (defaultLogger) Errorf(format string, args ...any) {
	log.Printf(format, args...)
}
