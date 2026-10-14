package tenant

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMiddlewareInjectsOrRejects(t *testing.T) {
	var seen string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { seen = From(r.Context()) })
	var rejected int
	reject := func(w http.ResponseWriter, status int, msg string) { rejected = status; w.WriteHeader(status) }

	h := Middleware(false, reject)(next)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(Header, "team-b")
	h.ServeHTTP(httptest.NewRecorder(), req)
	if seen != "team-b" || rejected != 0 {
		t.Fatalf("seen %q rejected %d", seen, rejected)
	}

	seen = ""
	h = Middleware(true, reject)(next)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rejected != http.StatusUnauthorized || seen != "" {
		t.Fatalf("required without header: rejected %d, handler saw %q", rejected, seen)
	}
}
