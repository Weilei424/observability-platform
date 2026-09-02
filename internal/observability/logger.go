package observability

import (
	"context"
	"fmt"
	"log/slog"
	"os"
)

func NewLogger(level string) (*slog.Logger, error) {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "info":
		l = slog.LevelInfo
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		return nil, fmt.Errorf("logger: unknown log level %q", level)
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l})
	return slog.New(handler), nil
}

// contextKey is unexported so no other package can collide with this key.
type contextKey struct{}

var loggerKey = contextKey{}

// Component returns a child logger that stamps every line with the component
// name. Call sites then stop hand-writing "component", "..." pairs — which is how
// the same subsystem ended up logging under several different names.
func Component(log *slog.Logger, name string) *slog.Logger {
	return log.With(slog.String("component", name))
}

// ContextWithLogger stores a request-scoped logger for handlers to retrieve.
func ContextWithLogger(ctx context.Context, log *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, log)
}

// FromContext returns the request-scoped logger, or the process default when the
// context carries none. Handlers also run outside a request — WAL replay, tests,
// the healthcheck probe — and must not panic there.
func FromContext(ctx context.Context) *slog.Logger {
	if log, ok := ctx.Value(loggerKey).(*slog.Logger); ok && log != nil {
		return log
	}
	return slog.Default()
}
