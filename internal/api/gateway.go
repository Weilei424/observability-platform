package api

import (
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/masonwheeler/observability-platform/internal/observability"
)

// Upstreams makes a Server a gateway: it serves exactly the public route table
// all-in-one serves, proxying each read route to the querier; write routes are
// served locally and routed through Deps.Writes. Nothing under /internal is in
// that table, so the gateway can never be used to reach a component's internal
// API.
//
// The Querier URL is required, and Routes on the same Deps must be RoutesAll (the
// zero value) -- New panics otherwise, rather than silently building a
// gateway that proxies only some of the public routes or that dereferences a
// nil target on the first request.
type Upstreams struct {
	Querier *url.URL
	// Transport carries proxied requests; nil means one with a 5 s dial timeout.
	Transport http.RoundTripper
}

// gatewayProxies are the two proxies a gateway routes reads through: one per
// error shape, because an unreachable querier must answer in the shape its
// route family's clients parse.
type gatewayProxies struct {
	promRead, lokiRead http.Handler
}

func newGatewayProxies(u *Upstreams) *gatewayProxies {
	transport := u.Transport
	if transport == nil {
		transport = &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeout:     90 * time.Second,
		}
	}
	return &gatewayProxies{
		promRead: abortOnClientGone(newUpstreamProxy("querier", u.Querier, transport, func(w http.ResponseWriter) {
			writePromError(w, http.StatusServiceUnavailable, "unavailable", "querier unavailable")
		})),
		lokiRead: abortOnClientGone(newUpstreamProxy("querier", u.Querier, transport, func(w http.ResponseWriter) {
			writeLokiError(w, http.StatusServiceUnavailable, "querier unavailable")
		})),
	}
}

func newUpstreamProxy(name string, target *url.URL, transport http.RoundTripper, unavailable func(http.ResponseWriter)) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			// ReverseProxy "cleans" a raw query it cannot fully parse (a `;`
			// separator, a malformed `%` escape) by silently re-encoding it
			// before Rewrite even runs (net/http/httputil.cleanQueryParams),
			// which can reorder or drop pairs outright. All-in-one parses the
			// query itself and answers 400 for exactly that input; restoring
			// the inbound request's own raw query here undoes that mutation,
			// so the upstream sees the same bytes the client sent.
			pr.Out.URL.RawQuery = pr.In.URL.RawQuery
			// One ID for the whole request: chi's RequestID middleware on the
			// upstream adopts an incoming X-Request-Id, so the gateway's access
			// line and the upstream's share a request_id.
			if id := chimiddleware.GetReqID(pr.In.Context()); id != "" {
				pr.Out.Header.Set(chimiddleware.RequestIDHeader, id)
			}
		},
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log := observability.Component(observability.FromContext(r.Context()), "gateway")
			// A caller that left (Grafana abandoning a dashboard refresh or
			// zoom) is not an outage: answer it the same 499 the query
			// handlers use for the same thing, logged at Debug, never as an
			// ERROR-logged 503 -- that would both misreport a live upstream as
			// down and inflate the self-observability dashboard's error rate
			// for something that never failed. r.Context().Err() is checked
			// too, not just err: a client that drops mid-upload can reach here
			// as a request-body read error rather than one that wraps
			// context.Canceled directly, and the inbound context is still the
			// tell that the client, not the upstream, is why this failed.
			if isCanceled(err) || isCanceled(r.Context().Err()) {
				log.Debug("upstream request canceled", "upstream", name, "err", err)
				w.WriteHeader(statusClientClosedConnection)
				return
			}
			log.Error("upstream unavailable", "upstream", name, "err", err)
			unavailable(w)
		},
	}
}

// abortOnClientGone wraps a proxy handler so that ReverseProxy's mid-stream
// abort is not misrecorded by the access-log and metrics middleware as a 500.
// Once headers are already written, a copy failure while streaming the
// response body makes ReverseProxy panic with http.ErrAbortHandler instead of
// calling ErrorHandler -- net/http's own recovery handles that panic quietly,
// but chi's middleware defers run first during the unwind and, without this,
// would see the handler as never having completed and record a 500 for it.
//
// The panic is recovered here -- before it ever reaches those defers -- only
// when the inbound request's own context says the client is already gone: the
// status already sent to that client is whatever the upstream had answered,
// not a failure. If the upstream instead died mid-body while the client was
// still connected and waiting, the same panic is re-raised unchanged: that is
// a real failure, and the client's connection still must be aborted, which is
// exactly what letting http.ErrAbortHandler continue to net/http's own
// recovery does.
func abortOnClientGone(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			if rec == http.ErrAbortHandler && isCanceled(r.Context().Err()) {
				return
			}
			panic(rec)
		}()
		next.ServeHTTP(w, r)
	})
}
