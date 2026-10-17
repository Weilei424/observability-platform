package rpc

import (
	"net/http"
	"strings"

	"github.com/masonwheeler/observability-platform/internal/tenant"
)

// RequireTenant guards the internal read and push routes: a request without
// X-Scope-OrgID is a peer bug, refused with 400 rather than read as the
// default tenant, which could hand one tenant's data to another. The value is
// validated like a public one.
func RequireTenant(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := strings.TrimSpace(r.Header.Get(tenant.Header))
		if v == "" {
			writeError(w, http.StatusBadRequest, tenant.ErrMissing.Error())
			return
		}
		if err := tenant.Validate(v); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		next.ServeHTTP(w, r.WithContext(tenant.Inject(r.Context(), v)))
	})
}
