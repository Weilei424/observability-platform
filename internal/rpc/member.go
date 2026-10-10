package rpc

import (
	"fmt"
	"net/http"
)

// MemberHeader carries the base URL a client meant to reach, as the ring lists
// it. A connection can end up at a different member than its URL names: when
// containers restart, Docker's DNS can briefly answer one member's name with
// an address another has just taken, and keep-alive then holds that
// connection. Two members' clients would then reach one ingester, and its one
// acknowledgement would count twice toward a write quorum.
const MemberHeader = "X-Obs-Member"

// RequireMember refuses, with 421 Misdirected Request, a request whose
// MemberHeader names a member other than self, the ingester's own normalized
// URL. A request without the header -- from a client before this check, or a
// person with curl -- passes. The client counts a 421 as an outage and drops
// its pooled connections, so its next request dials, and resolves, afresh.
func RequireMember(self string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if m := r.Header.Get(MemberHeader); m != "" && m != self {
				writeError(w, http.StatusMisdirectedRequest, fmt.Sprintf("this ingester is %s, not %s", self, m))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
