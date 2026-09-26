package api

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/masonwheeler/observability-platform/internal/observability"
)

// Upstreams makes a Server a gateway: it serves exactly the public route table
// all-in-one serves, proxying each write route to the ingester and each read
// route to the querier. Nothing under /internal is in that table, so the
// gateway can never be used to reach a component's internal API.
type Upstreams struct {
	Ingester *url.URL
	Querier  *url.URL
	// Transport carries proxied requests; nil means one with a 5 s dial timeout.
	Transport http.RoundTripper
}

// gatewayProxies are the three proxies a gateway routes through: one per
// upstream and per error shape, because an unreachable upstream must answer in
// the shape its route family's clients parse.
type gatewayProxies struct {
	write, promRead, lokiRead http.Handler
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
		write: newUpstreamProxy("ingester", u.Ingester, transport, func(w http.ResponseWriter) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "ingester unavailable"})
		}),
		promRead: newUpstreamProxy("querier", u.Querier, transport, func(w http.ResponseWriter) {
			writePromError(w, http.StatusServiceUnavailable, "unavailable", "querier unavailable")
		}),
		lokiRead: newUpstreamProxy("querier", u.Querier, transport, func(w http.ResponseWriter) {
			writeLokiError(w, http.StatusServiceUnavailable, "querier unavailable")
		}),
	}
}

func newUpstreamProxy(name string, target *url.URL, transport http.RoundTripper, unavailable func(http.ResponseWriter)) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			// One ID for the whole request: chi's RequestID middleware on the
			// upstream adopts an incoming X-Request-Id, so the gateway's access
			// line and the upstream's share a request_id.
			if id := chimiddleware.GetReqID(pr.In.Context()); id != "" {
				pr.Out.Header.Set(chimiddleware.RequestIDHeader, id)
			}
		},
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if errors.Is(err, context.Canceled) {
				return // the client left; there is no one to answer and nothing is down
			}
			observability.Component(observability.FromContext(r.Context()), "gateway").Warn(
				"upstream unavailable", "upstream", name, "err", err)
			unavailable(w)
		},
	}
}
