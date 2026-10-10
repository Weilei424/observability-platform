package rpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
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

// errorBodyLimit bounds how much of a non-200 answer the client buffers. An
// error body is always small — an {"error": "..."} object, or a maintenance
// route's partial-count-plus-error — never data proportional to what was
// requested, so a misbehaving peer can't make the client buffer an unbounded
// body.
const errorBodyLimit = 1 << 20

// Client calls one peer's /internal/v1 API.
type Client struct {
	peer string
	base *url.URL
	http *http.Client
	// timeout bounds each request when positive; zero leaves a request to its
	// own context.
	timeout time.Duration
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithRequestTimeout bounds every request to d, on top of the caller's own
// context. A request that outlives it fails as ErrUnavailable, the same as any
// peer that fails to answer in time.
func WithRequestTimeout(d time.Duration) ClientOption {
	return func(c *Client) { c.timeout = d }
}

// NewClient returns a client for the peer at base, a base http(s) URL that
// config has already validated. peer names the component in errors.
func NewClient(peer, base string, opts ...ClientOption) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("rpc: %s URL %q: %w", peer, base, err)
	}
	c := &Client{
		peer: peer,
		base: u,
		http: &http.Client{Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: DialTimeout, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: DialTimeout,
		}},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// do sends in (JSON, when non-nil) to /internal/v1/<path> and decodes the
// 200 answer into out. A transport failure, a context deadline, or a 5xx is
// ErrUnavailable — a deadline means the peer failed to answer in time, the
// same as any other timeout; any other status, 204 included, is a protocol
// disagreement between two components and is returned as-is. On an error answer out is
// still filled when the body decodes, so a partial result — compaction and
// retention report one — survives. A cancellation is different from a
// deadline: it is the caller giving up on its own, not the peer failing to
// answer, so it is returned as the context's own error and is not ErrUnavailable.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, in, out any) error {
	return c.call(ctx, method, path, query, in, out, http.StatusOK)
}

// doNoContent is do for a route whose only success is 204 with no body: the
// push routes. A 200 there is as much a protocol disagreement as a 204 where
// do expects a body — each operation accepts exactly its own success status,
// so a 204 can never pass for an empty read or an acknowledged flush.
func (c *Client) doNoContent(ctx context.Context, method, path string, in any) error {
	return c.call(ctx, method, path, nil, in, nil, http.StatusNoContent)
}

func (c *Client) call(ctx context.Context, method, path string, query url.Values, in, out any, want int) error {
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
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
		if cerr := c.ctxErr(ctx, path); cerr != nil {
			return cerr
		}
		return fmt.Errorf("%w: %s %s: %v", ErrUnavailable, c.peer, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != want {
		// Cap what an error answer buffers: it is always small (an
		// {"error": ...} object, or a maintenance route's partial count), so a
		// misbehaving peer can't make the client hold an unbounded body in
		// memory. Draining whatever is left over the cap afterward lets the
		// Transport reuse the connection instead of closing it, the same as a
		// full read would.
		raw, err := io.ReadAll(io.LimitReader(resp.Body, errorBodyLimit))
		if err != nil {
			if cerr := c.ctxErr(ctx, path); cerr != nil {
				return cerr
			}
			return fmt.Errorf("%w: %s %s: reading the answer: %v", ErrUnavailable, c.peer, path, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		if out != nil {
			_ = json.Unmarshal(raw, out)
		}
		// A 5xx is the peer failing; a 499 is the peer having seen this
		// request as abandoned (statusClientClosedRequest) -- neither is the
		// two sides disagreeing about the protocol, so both are an outage.
		if resp.StatusCode >= 500 || resp.StatusCode == statusClientClosedRequest {
			return fmt.Errorf("%w: %s %s answered %d: %s", ErrUnavailable, c.peer, path, resp.StatusCode, errorMessage(raw))
		}
		return fmt.Errorf("rpc: %s %s answered %d: %s", c.peer, path, resp.StatusCode, errorMessage(raw))
	}

	if want == http.StatusNoContent {
		return nil
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		if cerr := c.ctxErr(ctx, path); cerr != nil {
			return cerr
		}
		return fmt.Errorf("%w: %s %s: reading the answer: %v", ErrUnavailable, c.peer, path, err)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("rpc: decode %s %s answer: %w", c.peer, path, err)
		}
	}
	return nil
}

// ctxErr classifies ctx's own error for a request to path, or returns nil when
// ctx carries none. A deadline means the peer failed to answer in time, the
// same as any other timeout, so it counts as ErrUnavailable; a cancellation is
// the caller giving up on its own, not the peer's fault, so it is returned as
// the caller's own error (wrapped only with the peer and route) and does not
// count as an outage.
func (c *Client) ctxErr(ctx context.Context, path string) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %s %s: %w", ErrUnavailable, c.peer, path, err)
	}
	return fmt.Errorf("rpc: %s %s: %w", c.peer, path, err)
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
