// Package tenant carries the tenant a request acts for (Phase 6.5): its ID,
// validation, the request context, and the public HTTP middleware.
package tenant

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

const (
	// Default is the tenant of a request without X-Scope-OrgID. Its series and
	// streams carry no Label, so every pre-6.5 byte is the default tenant's.
	Default = "anonymous"
	// Header names the tenant, as in Mimir and Loki.
	Header = "X-Scope-OrgID"
	// Label is the reserved label that names a non-default tenant inside a
	// series or stream label set.
	Label = "__tenant__"
	// MaxLength is the longest tenant ID.
	MaxLength = 150
)

// ErrMissing is an internal request that arrived without Header.
var ErrMissing = errors.New("missing tenant")

// Validate reports whether id is a usable tenant ID: 1-150 bytes of
// [A-Za-z0-9!_.*'()-], and not "." or "..".
func Validate(id string) error {
	if id == "" {
		return errors.New("invalid tenant ID: empty")
	}
	if len(id) > MaxLength {
		return fmt.Errorf("invalid tenant ID: longer than %d bytes", MaxLength)
	}
	if id == "." || id == ".." {
		return fmt.Errorf("invalid tenant ID %q", id)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case strings.IndexByte("!-_.*'()", c) >= 0:
		default:
			return fmt.Errorf("invalid tenant ID %q: character %q is not allowed", id, c)
		}
	}
	return nil
}

type ctxKey struct{}

// Inject returns ctx acting for tenant id.
func Inject(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// From returns the tenant ctx acts for, or Default when none was injected.
func From(ctx context.Context) string {
	if id, ok := ctx.Value(ctxKey{}).(string); ok && id != "" {
		return id
	}
	return Default
}

// Resolve reads the tenant from a public request's headers. status is 0 when
// id is usable, otherwise the status to answer with and msg the reason. An
// empty header value is the same as no header.
func Resolve(h http.Header, required bool) (id string, status int, msg string) {
	v := strings.TrimSpace(h.Get(Header))
	if v == "" {
		if required {
			return "", http.StatusUnauthorized, "no tenant: set the " + Header + " header"
		}
		return Default, 0, ""
	}
	if strings.Contains(v, "|") {
		return "", http.StatusBadRequest, "multi-tenant queries are not supported"
	}
	if err := Validate(v); err != nil {
		return "", http.StatusBadRequest, err.Error()
	}
	return v, 0, ""
}
