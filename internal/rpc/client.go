package rpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	chimiddleware "github.com/go-chi/chi/v5/middleware"
)

// DialTimeout bounds connecting to a peer. A request's own deadline comes from
// its context.
const DialTimeout = 5 * time.Second

// Client calls one peer's /internal/v1 API.
type Client struct {
	peer string
	base *url.URL
	http *http.Client
}

// NewClient returns a client for the peer at base, a base http(s) URL that
// config has already validated. peer names the component in errors.
func NewClient(peer, base string) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("rpc: %s URL %q: %w", peer, base, err)
	}
	return &Client{
		peer: peer,
		base: u,
		http: &http.Client{Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: DialTimeout, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     90 * time.Second,
		}},
	}, nil
}

// do sends in (JSON, when non-nil) to /internal/v1/<path> and decodes the
// answer into out. A transport failure or a 5xx is ErrUnavailable; any other
// non-200 is a protocol disagreement between two components and is returned
// as-is. On an error answer out is still filled when the body decodes, so a
// partial result — compaction and retention report one — survives.
// Cancellation is returned as the context's own error: a caller that gave up is
// not an outage.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, in, out any) error {
	var body io.Reader
	if in != nil {
		// marshalJSON, not json.Marshal: it is the one encoding path for every
		// internal body (codec.go). encoding/json's default HTML escaping would
		// turn '<', '>', '&' in a log line or label value into a six-byte
		// \u00XX escape each, which the ingester's flush-batch sizing does not
		// account for.
		b, err := marshalJSON(in)
		if err != nil {
			return fmt.Errorf("rpc: encode %s request: %w", path, err)
		}
		body = bytes.NewReader(b)
	}
	u := c.base.JoinPath("internal", "v1", path)
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return fmt.Errorf("rpc: %s %s: %w", c.peer, path, err)
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set(chimiddleware.RequestIDHeader, requestID(ctx))

	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %s %s: %v", ErrUnavailable, c.peer, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %s %s: reading the answer: %v", ErrUnavailable, c.peer, path, err)
	}
	if resp.StatusCode != http.StatusOK {
		if out != nil {
			_ = json.Unmarshal(raw, out)
		}
		if resp.StatusCode >= 500 {
			return fmt.Errorf("%w: %s %s answered %d: %s", ErrUnavailable, c.peer, path, resp.StatusCode, errorMessage(raw))
		}
		return fmt.Errorf("rpc: %s %s answered %d: %s", c.peer, path, resp.StatusCode, errorMessage(raw))
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("rpc: decode %s %s answer: %w", c.peer, path, err)
		}
	}
	return nil
}

// requestID forwards the inbound request's ID, so one ID follows a query from
// the gateway through the querier to the ingester and store. Work with no
// inbound request — a flush, a maintenance pass — gets its own.
func requestID(ctx context.Context) string {
	if id := chimiddleware.GetReqID(ctx); id != "" {
		return id
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "rpc-" + hex.EncodeToString(b[:])
}
