package tenant

import "net/http"

// Middleware resolves the tenant of a public request and injects it, or answers
// through reject: 401 when required and absent, 400 when invalid. reject writes
// the route family's own error shape (Prometheus JSON, Loki text, write JSON).
func Middleware(required bool, reject func(w http.ResponseWriter, status int, msg string)) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id, status, msg := Resolve(r.Header, required)
			if status != 0 {
				reject(w, status, msg)
				return
			}
			next.ServeHTTP(w, r.WithContext(Inject(r.Context(), id)))
		})
	}
}
