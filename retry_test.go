// Copyright 2014 Martin Angers and Contributors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fetchbot

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// statusDoer is a test Doer that returns scripted status codes and records
// how many requests it served. An optional Retry-After header can be set on
// the retryable responses.
type statusDoer struct {
	mu       sync.Mutex
	calls    int32
	statuses []int
	headers  []http.Header // optional, same length as statuses; nil entries are fine
	bodies   []string
	gotReqs  []*http.Request
	err      error
}

func (d *statusDoer) Do(req *http.Request) (*http.Response, error) {
	n := int(atomic.AddInt32(&d.calls, 1)) - 1
	d.mu.Lock()
	d.gotReqs = append(d.gotReqs, req)
	d.mu.Unlock()
	if d.err != nil {
		return nil, d.err
	}
	idx := n
	if idx >= len(d.statuses) {
		idx = len(d.statuses) - 1
	}
	status := d.statuses[idx]
	body := ""
	if idx < len(d.bodies) {
		body = d.bodies[idx]
	}
	res := &http.Response{
		Status:     fmt.Sprintf("%d status", status),
		StatusCode: status,
		Request:    req,
		Header:     http.Header{},
		Body:       ioutil.NopCloser(strings.NewReader(body)),
	}
	if idx < len(d.headers) && d.headers[idx] != nil {
		for k, vs := range d.headers[idx] {
			for _, v := range vs {
				res.Header.Add(k, v)
			}
		}
	}
	return res, nil
}

func (d *statusDoer) Calls() int { return int(atomic.LoadInt32(&d.calls)) }

// trackBody is an io.ReadCloser recording whether it was closed.
type trackBody struct {
	io.Reader
	mu     sync.Mutex
	closed int32
}

func (b *trackBody) Close() error {
	atomic.StoreInt32(&b.closed, 1)
	return nil
}

func (b *trackBody) Closed() bool { return atomic.LoadInt32(&b.closed) == 1 }

// waitForSignal waits up to timeout for ch to be closed, failing the test
// on timeout. Tests use synchronization signals instead of fixed sleeps.
func waitForSignal(t *testing.T, ch <-chan struct{}, what string, timeout time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s after %s", what, timeout)
	}
}

// retryFetcher builds a Fetcher configured for fast, deterministic retry
// tests: politeness and crawl delay are disabled and the given Doer is used.
func retryFetcher(h Handler, d Doer) *Fetcher {
	f := New(h)
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.HttpClient = d
	f.MaxRetries = 3
	f.RetryDelay = 20 * time.Millisecond
	f.MaxRetryDelay = 100 * time.Millisecond
	return f
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u
}

func httptestNewServer(t *testing.T, h func(http.ResponseWriter, *http.Request)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(h))
}

// headerAuthCookieCmd provides headers, basic auth and cookies but no
// request body, so it is retry-eligible.
type headerAuthCookieCmd struct {
	*Cmd
}

func (c *headerAuthCookieCmd) Header() http.Header {
	return http.Header{"X-Test": {"v"}}
}

func (c *headerAuthCookieCmd) BasicAuth() (string, string) {
	return "me", "you"
}

func (c *headerAuthCookieCmd) Cookies() []*http.Cookie {
	return []*http.Cookie{{Name: "c", Value: "1"}}
}

// TestRetryBasic verifies that a 429 followed by a 200 leads to one retry,
// a single callback with the final status code and body, and the retry
// delay being honored.
func TestRetryBasic(t *testing.T) {
	done := make(chan struct{})
	var gotStatus int
	var gotBody string
	var calls int32
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		gotStatus = res.StatusCode
		b, _ := ioutil.ReadAll(res.Body)
		gotBody = string(b)
		atomic.AddInt32(&calls, 1)
		close(done)
	})
	doer := &statusDoer{
		statuses: []int{http.StatusTooManyRequests, http.StatusOK},
		bodies:   []string{"", "hello"},
	}
	f := retryFetcher(sh, doer)
	q := f.Start()
	if _, err := q.SendStringGet("http://example.com/a"); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, done, "handler callback", 5*time.Second)
	q.Close()

	if doer.Calls() != 2 {
		t.Errorf("expected 2 HTTP requests (1 + 1 retry), got %d", doer.Calls())
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("expected exactly 1 handler callback, got %d", got)
	}
	if gotStatus != http.StatusOK {
		t.Errorf("expected final status 200, got %d", gotStatus)
	}
	if gotBody != "hello" {
		t.Errorf("expected final body %q, got %q", "hello", gotBody)
	}
}

// TestRetryOn503 verifies the same behavior for the 503 status code.
func TestRetryOn503(t *testing.T) {
	done := make(chan struct{})
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		if res.StatusCode != http.StatusOK {
			t.Errorf("expected final status 200, got %d", res.StatusCode)
		}
		close(done)
	})
	doer := &statusDoer{statuses: []int{http.StatusServiceUnavailable, http.StatusOK}}
	f := retryFetcher(sh, doer)
	q := f.Start()
	q.SendStringGet("http://example.com/a")
	waitForSignal(t, done, "handler callback", 5*time.Second)
	q.Close()

	if doer.Calls() != 2 {
		t.Errorf("expected 2 HTTP requests, got %d", doer.Calls())
	}
}

// TestRetryExhausted verifies that once MaxRetries is used up, the final
// 429/503 response is delivered to the Handler and the retry-derived wait
// still delays the next command on the same host.
func TestRetryExhausted(t *testing.T) {
	done := make(chan struct{})
	var mu sync.Mutex
	var order []string
	firstDone := make(chan struct{})
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		mu.Lock()
		order = append(order, ctx.Cmd.URL().Path)
		mu.Unlock()
		if ctx.Cmd.URL().Path == "/a" {
			if res.StatusCode != http.StatusServiceUnavailable {
				t.Errorf("expected final status 503, got %d", res.StatusCode)
			}
			close(firstDone)
		}
		if ctx.Cmd.URL().Path == "/b" {
			close(done)
		}
	})
	// /a always fails; MaxRetries is 2 so it gets 3 attempts and waits
	// ~20ms and ~40ms; the last failed response imposes another ~80ms wait.
	doer := doerFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Request:    req,
			Header:     http.Header{},
			Body:       ioutil.NopCloser(strings.NewReader("")),
		}, nil
	})
	f := New(sh)
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.HttpClient = doer
	f.MaxRetries = 2
	f.RetryDelay = 20 * time.Millisecond
	f.MaxRetryDelay = time.Second
	q := f.Start()
	q.SendStringGet("http://example.com/a")
	<-firstDone
	start := time.Now()
	q.SendStringGet("http://example.com/b")
	waitForSignal(t, done, "second command callback", 5*time.Second)
	elapsed := time.Since(start)
	q.Close()

	// /a: 3 requests; /b: 1 request.
	// The post-wait after the exhausted command must delay /b.
	if elapsed < 70*time.Millisecond {
		t.Errorf("expected /b to wait for the last retry-derived delay, got %s", elapsed)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "/a" || order[1] != "/b" {
		t.Errorf("expected order [/a /b], got %v", order)
	}
}

// TestRetryDisabledByDefault verifies that a freshly created Fetcher does
// not retry, even when the server answers 429/503.
func TestRetryDisabledByDefault(t *testing.T) {
	done := make(chan struct{})
	var calls int32
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		if res.StatusCode != http.StatusTooManyRequests {
			t.Errorf("expected status 429, got %d", res.StatusCode)
		}
		atomic.AddInt32(&calls, 1)
		close(done)
	})
	doer := &statusDoer{statuses: []int{http.StatusTooManyRequests, http.StatusOK}}
	f := New(sh)
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.HttpClient = doer
	q := f.Start()
	q.SendStringGet("http://example.com/a")
	waitForSignal(t, done, "handler callback", 5*time.Second)
	q.Close()

	if doer.Calls() != 1 {
		t.Errorf("expected a single request with retries disabled, got %d", doer.Calls())
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("expected 1 callback, got %d", got)
	}
}

// TestRetryEligibility verifies which commands are eligible for retries.
func TestRetryEligibility(t *testing.T) {
	parsed := mustParseURL(t, "http://example.com/a")
	cases := []struct {
		name string
		cmd  Command
		want bool
	}{
		{"GET cmd", &Cmd{U: parsed, M: "GET"}, true},
		{"HEAD cmd", &Cmd{U: parsed, M: "HEAD"}, true},
		{"lowercase get", &Cmd{U: parsed, M: "get"}, false},
		{"POST cmd", &Cmd{U: parsed, M: "POST"}, false},
		{"PUT cmd", &Cmd{U: parsed, M: "PUT"}, false},
		{"DELETE cmd", &Cmd{U: parsed, M: "DELETE"}, false},
		{"reader body", &readerCmd{&Cmd{U: parsed, M: "GET"}, strings.NewReader("x")}, false},
		{"values body", &valuesCmd{&Cmd{U: parsed, M: "GET"}, url.Values{"a": {"b"}}}, false},
		{"robots request", robotCommand{&Cmd{U: parsed, M: "GET"}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := canRetryCommand(c.cmd); got != c.want {
				t.Errorf("canRetryCommand(%s) = %v, want %v", c.name, got, c.want)
			}
		})
	}
}

// TestRetryNonEligibleNotRetried verifies end-to-end that POST commands and
// commands with a body are served exactly once even on 429.
func TestRetryNonEligibleNotRetried(t *testing.T) {
	for _, tc := range []struct {
		name   string
		build  func(u *url.URL) Command
		method string
	}{
		{"POST", func(u *url.URL) Command { return &Cmd{U: u, M: "POST"} }, "POST"},
		{"reader body", func(u *url.URL) Command {
			return &readerCmd{&Cmd{U: u, M: "POST"}, strings.NewReader("body")}
		}, "POST"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			done := make(chan struct{})
			sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
				close(done)
			})
			doer := &statusDoer{statuses: []int{http.StatusTooManyRequests, http.StatusOK}}
			f := retryFetcher(sh, doer)
			q := f.Start()
			if err := q.Send(tc.build(mustParseURL(t, "http://example.com/a"))); err != nil {
				t.Fatal(err)
			}
			waitForSignal(t, done, "handler callback", 5*time.Second)
			q.Close()
			if doer.Calls() != 1 {
				t.Errorf("expected 1 request for %s, got %d", tc.name, doer.Calls())
			}
		})
	}
}

// TestRetryNetworkErrorNotRetried verifies that network errors bypass the
// retry machinery and are delivered straight to the Handler.
func TestRetryNetworkErrorNotRetried(t *testing.T) {
	done := make(chan struct{})
	wantErr := fmt.Errorf("connection refused")
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		if err != wantErr {
			t.Errorf("expected error %v, got %v", wantErr, err)
		}
		close(done)
	})
	doer := &statusDoer{err: wantErr}
	f := retryFetcher(sh, doer)
	q := f.Start()
	q.SendStringGet("http://example.com/a")
	waitForSignal(t, done, "handler callback", 5*time.Second)
	q.Close()
	if doer.Calls() != 1 {
		t.Errorf("expected 1 request on network error, got %d", doer.Calls())
	}
}

// TestRetryBackoff verifies the exponential doubling and the cap.
func TestRetryBackoff(t *testing.T) {
	// Unit-level checks on the doubling helper.
	d := doubleBackoff(20*time.Millisecond, 100*time.Millisecond)
	if d != 40*time.Millisecond {
		t.Errorf("expected 40ms, got %s", d)
	}
	d = doubleBackoff(60*time.Millisecond, 100*time.Millisecond)
	if d != 100*time.Millisecond {
		t.Errorf("expected capped 100ms, got %s", d)
	}
	d = doubleBackoff(100*time.Millisecond, 100*time.Millisecond)
	if d != 100*time.Millisecond {
		t.Errorf("expected capped 100ms, got %s", d)
	}

	// End-to-end: 4 consecutive failures (initial request + 3 retries).
	// The retry waits are 20ms, 40ms and then a capped 100ms (the doubled
	// 80ms exceeds the 100ms cap), so the command takes at least ~160ms.
	done := make(chan struct{})
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		close(done)
	})
	doer := doerFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Request:    req,
			Header:     http.Header{},
			Body:       ioutil.NopCloser(strings.NewReader("")),
		}, nil
	})
	f := New(sh)
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.HttpClient = doer
	f.MaxRetries = 3
	f.RetryDelay = 20 * time.Millisecond
	f.MaxRetryDelay = 100 * time.Millisecond
	start := time.Now()
	q := f.Start()
	q.SendStringGet("http://example.com/a")
	waitForSignal(t, done, "handler callback", 5*time.Second)
	elapsed := time.Since(start)
	q.Close()
	// Nominal waits: 20ms + 40ms + 100ms (capped). The lower bound only
	// guards against skipping waits; timer granularity on some platforms
	// may wake slightly early. An upper bound catches a regression that
	// ignores the cap (which would add another 160ms instead of 100ms).
	if elapsed < 120*time.Millisecond || elapsed > 400*time.Millisecond {
		t.Errorf("expected exponential waits near 160ms, got %s", elapsed)
	}
}

// TestRetryWaitCap verifies that retryWait always honors MaxRetryDelay,
// including when Retry-After asks for a huge number of seconds.
func TestRetryWaitCap(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "999999")
	if d := retryWait(h, time.Second, 100*time.Millisecond); d != 100*time.Millisecond {
		t.Errorf("expected cap 100ms, got %s", d)
	}
	// No header -> backoff, also capped.
	if d := retryWait(http.Header{}, 200*time.Millisecond, 100*time.Millisecond); d != 100*time.Millisecond {
		t.Errorf("expected capped backoff 100ms, got %s", d)
	}
}

// TestParseRetryAfterTable covers seconds, HTTP dates, whitespace, invalid
// values, negative numbers and past dates.
func TestParseRetryAfterTable(t *testing.T) {
	future := time.Now().Add(2 * time.Second).UTC().Format(http.TimeFormat)
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	cases := []struct {
		name string
		in   string
		ok   bool
		want time.Duration
	}{
		{"zero seconds", "0", true, 0},
		{"plain seconds", "5", true, 5 * time.Second},
		{"whitespace seconds", "  3  ", true, 3 * time.Second},
		{"large seconds", "3600", true, time.Hour},
		{"negative", "-2", false, 0},
		{"decimal", "1.5", false, 0},
		{"junk", "soon", false, 0},
		{"empty", "", false, 0},
		{"whitespace only", "   ", false, 0},
		{"http date future", future, true, 0}, // asserted with tolerance below
		{"http date past", past, true, 0},
		{"http date with whitespace", "  " + future + "  ", true, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := parseRetryAfter(c.in)
			if ok != c.ok {
				t.Fatalf("parseRetryAfter(%q) ok = %v, want %v", c.in, ok, c.ok)
			}
			if !ok {
				return
			}
			if c.name == "http date future" || c.name == "http date with whitespace" {
				// Expect roughly 2 seconds, allow slack for scheduling.
				if got < time.Second || got > 3*time.Second {
					t.Errorf("expected ~2s for future date, got %s", got)
				}
				return
			}
			if got != c.want {
				t.Errorf("parseRetryAfter(%q) = %s, want %s", c.in, got, c.want)
			}
		})
	}
}

// TestRetryAfterHeaderEndToEnd verifies that valid Retry-After values
// replace the backoff, invalid ones fall back to it, and "0" means an
// immediate retry.
func TestRetryAfterHeaderEndToEnd(t *testing.T) {
	// Sub-case 1: Retry-After: 0 -> immediate retry, faster than backoff.
	{
		done := make(chan struct{})
		sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) { close(done) })
		doer := &statusDoer{
			statuses: []int{http.StatusServiceUnavailable, http.StatusOK},
			headers:  []http.Header{{"Retry-After": {"0"}}, nil},
		}
		f := retryFetcher(sh, doer)
		f.RetryDelay = time.Second // would be slow if the header were ignored
		start := time.Now()
		q := f.Start()
		q.SendStringGet("http://example.com/a")
		waitForSignal(t, done, "handler callback", 5*time.Second)
		elapsed := time.Since(start)
		q.Close()
		if elapsed > 100*time.Millisecond {
			t.Errorf("Retry-After: 0 should retry immediately, took %s", elapsed)
		}
		if doer.Calls() != 2 {
			t.Errorf("expected 2 requests, got %d", doer.Calls())
		}
	}

	// Sub-case 2: invalid header -> fall back to RetryDelay.
	{
		done := make(chan struct{})
		sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) { close(done) })
		doer := &statusDoer{
			statuses: []int{http.StatusServiceUnavailable, http.StatusOK},
			headers:  []http.Header{{"Retry-After": {"not-a-date-or-number"}}, nil},
		}
		f := retryFetcher(sh, doer)
		f.RetryDelay = 80 * time.Millisecond
		start := time.Now()
		q := f.Start()
		q.SendStringGet("http://example.com/a")
		waitForSignal(t, done, "handler callback", 5*time.Second)
		elapsed := time.Since(start)
		q.Close()
		if elapsed < 60*time.Millisecond {
			t.Errorf("invalid Retry-After should fall back to RetryDelay, took %s", elapsed)
		}
	}

	// Sub-case 3: oversized seconds -> capped at MaxRetryDelay.
	{
		done := make(chan struct{})
		sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) { close(done) })
		doer := &statusDoer{
			statuses: []int{http.StatusServiceUnavailable, http.StatusOK},
			headers:  []http.Header{{"Retry-After": {"3600"}}, nil},
		}
		f := retryFetcher(sh, doer)
		f.RetryDelay = 20 * time.Millisecond
		f.MaxRetryDelay = 50 * time.Millisecond
		start := time.Now()
		q := f.Start()
		q.SendStringGet("http://example.com/a")
		waitForSignal(t, done, "handler callback", 5*time.Second)
		elapsed := time.Since(start)
		q.Close()
		if elapsed < 40*time.Millisecond || elapsed > 300*time.Millisecond {
			t.Errorf("expected retry capped near 50ms, took %s", elapsed)
		}
	}
}

// TestSameHostOrdering verifies that later commands on the same host cannot
// overtake a command whose retries are still in progress.
func TestSameHostOrdering(t *testing.T) {
	var (
		mu        sync.Mutex
		order     []string
		aAttempts int
	)
	done := make(chan struct{})
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		mu.Lock()
		order = append(order, ctx.Cmd.URL().Path)
		n := len(order)
		mu.Unlock()
		if n == 3 {
			close(done)
		}
	})
	doer := doerFunc(func(req *http.Request) (*http.Response, error) {
		status := http.StatusOK
		if req.URL.Path == "/a" {
			// /a fails twice, then succeeds.
			mu.Lock()
			n := aAttempts
			aAttempts++
			mu.Unlock()
			if n < 2 {
				status = http.StatusTooManyRequests
			}
		}
		return &http.Response{
			StatusCode: status,
			Request:    req,
			Header:     http.Header{},
			Body:       ioutil.NopCloser(strings.NewReader("ok")),
		}, nil
	})
	f := New(sh)
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.HttpClient = doer
	f.MaxRetries = 5
	f.RetryDelay = 15 * time.Millisecond
	f.MaxRetryDelay = 100 * time.Millisecond
	q := f.Start()
	if _, err := q.SendStringGet("http://example.com/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.SendStringGet("http://example.com/b"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.SendStringGet("http://example.com/c"); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, done, "all callbacks", 5*time.Second)
	q.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != "/a" || order[1] != "/b" || order[2] != "/c" {
		t.Errorf("expected strict order [/a /b /c], got %v", order)
	}
}

// TestOtherHostNotBlocked verifies that a host stuck in retry waits does
// not delay commands on a different host.
func TestOtherHostNotBlocked(t *testing.T) {
	slowFirstAttempt := make(chan struct{})
	slowAttempts := int32(0)
	fastDone := make(chan struct{})
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		if ctx.Cmd.URL().Host == "fast.example" {
			close(fastDone)
		}
	})
	doer := doerFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == "slow.example" {
			if atomic.AddInt32(&slowAttempts, 1) == 1 {
				close(slowFirstAttempt)
			}
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Request:    req,
				Header:     http.Header{},
				Body:       ioutil.NopCloser(strings.NewReader("")),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Request:    req,
			Header:     http.Header{},
			Body:       ioutil.NopCloser(strings.NewReader("ok")),
		}, nil
	})
	f := New(sh)
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.HttpClient = doer
	f.MaxRetries = 3
	f.RetryDelay = 300 * time.Millisecond // slow host's waits are long
	f.MaxRetryDelay = time.Second
	q := f.Start()
	q.SendStringGet("http://slow.example/a")
	// Wait until the slow host has entered its (long) retry wait, then
	// enqueue the fast-host command.
	waitForSignal(t, slowFirstAttempt, "slow host first attempt", 5*time.Second)
	q.SendStringGet("http://fast.example/a")
	// The fast host must complete well before the slow host's 300ms wait
	// elapses; the generous timeout still catches a blocking regression.
	waitForSignal(t, fastDone, "fast host callback", 2*time.Second)
	if got := atomic.LoadInt32(&slowAttempts); got != 1 {
		t.Errorf("slow host should still be on its first wait, attempts=%d", got)
	}
	q.Cancel()
}

// TestIntermediateBodyClosed verifies that intermediate 429/503 response
// bodies are closed before the retry wait, while the final response body
// stays readable during the callback and is closed afterwards.
func TestIntermediateBodyClosed(t *testing.T) {
	done := make(chan struct{})
	var finalBody *trackBody
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		b, readErr := ioutil.ReadAll(res.Body)
		if readErr != nil {
			t.Errorf("final body should be readable in callback: %v", readErr)
		}
		if string(b) != "final" {
			t.Errorf("unexpected final body %q", string(b))
		}
		finalBody = res.Body.(*trackBody)
		close(done)
	})
	doer := doerFunc(func(req *http.Request) (*http.Response, error) {
		body := &trackBody{Reader: strings.NewReader("final")}
		return &http.Response{
			StatusCode: http.StatusOK,
			Request:    req,
			Header:     http.Header{},
			Body:       body,
		}, nil
	})
	intermediateBodies := []*trackBody{}
	var ibMu sync.Mutex
	// Wrap the doer to inject 429 responses with tracked bodies first.
	attempt := int32(0)
	wrapped := doerFunc(func(req *http.Request) (*http.Response, error) {
		n := atomic.AddInt32(&attempt, 1)
		if n == 1 {
			b := &trackBody{Reader: strings.NewReader("rate-limited")}
			ibMu.Lock()
			intermediateBodies = append(intermediateBodies, b)
			ibMu.Unlock()
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Request:    req,
				Header:     http.Header{},
				Body:       b,
			}, nil
		}
		return doer.Do(req)
	})
	f := retryFetcher(sh, wrapped)
	f.RetryDelay = 20 * time.Millisecond
	q := f.Start()
	q.SendStringGet("http://example.com/a")
	waitForSignal(t, done, "handler callback", 5*time.Second)
	q.Close()

	ibMu.Lock()
	for i, b := range intermediateBodies {
		if !b.Closed() {
			t.Errorf("intermediate body %d was not closed", i)
		}
	}
	ibMu.Unlock()
	// visit() closes the final body after the callback returns.
	if finalBody == nil || !finalBody.Closed() {
		t.Errorf("expected final body to be closed after the callback")
	}
}

// TestRetryCancelDuringWait verifies that Cancel interrupts a retry wait,
// prevents further attempts and suppresses the callback, while Close-style
// completion remains available.
func TestRetryCancelDuringWait(t *testing.T) {
	canceled := make(chan struct{})
	var callbacks int32
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		atomic.AddInt32(&callbacks, 1)
	})
	firstAttempt := make(chan struct{})
	doer := doerFunc(func(req *http.Request) (*http.Response, error) {
		select {
		case <-firstAttempt:
		default:
			close(firstAttempt)
		}
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Request:    req,
			Header:     http.Header{},
			Body:       ioutil.NopCloser(strings.NewReader("")),
		}, nil
	})
	f := New(sh)
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.HttpClient = doer
	f.MaxRetries = 5
	f.RetryDelay = 10 * time.Second // long wait, must be interrupted by Cancel
	f.MaxRetryDelay = time.Minute
	q := f.Start()
	q.SendStringGet("http://example.com/a")
	waitForSignal(t, firstAttempt, "first HTTP attempt", 5*time.Second)

	go func() {
		q.Cancel()
		close(canceled)
	}()
	waitForSignal(t, canceled, "Cancel to return", 5*time.Second)

	if got := atomic.LoadInt32(&callbacks); got != 0 {
		t.Errorf("expected no callback after cancellation, got %d", got)
	}
}

// TestRetryCloseWaitsForRetries verifies that Queue.Close waits for an
// accepted command and its retries to complete normally.
func TestRetryCloseWaitsForRetries(t *testing.T) {
	var callbacks int32
	var requests int32
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		atomic.AddInt32(&callbacks, 1)
	})
	doer := doerFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&requests, 1)
		n := atomic.LoadInt32(&requests)
		status := http.StatusTooManyRequests
		if n >= 3 {
			status = http.StatusOK
		}
		return &http.Response{
			StatusCode: status,
			Request:    req,
			Header:     http.Header{},
			Body:       ioutil.NopCloser(strings.NewReader("")),
		}, nil
	})
	f := New(sh)
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.HttpClient = doer
	f.MaxRetries = 2
	f.RetryDelay = 15 * time.Millisecond
	f.MaxRetryDelay = 100 * time.Millisecond
	q := f.Start()
	q.SendStringGet("http://example.com/a")
	q.Close() // must block until retries finish

	if got := atomic.LoadInt32(&requests); got != 3 {
		t.Errorf("expected 3 HTTP requests after Close, got %d", got)
	}
	if got := atomic.LoadInt32(&callbacks); got != 1 {
		t.Errorf("expected 1 final callback after Close, got %d", got)
	}
}

// TestRetryNotLostOnWorkerIdle verifies that a command waiting between
// retries is not dropped when its wait exceeds WorkerIdleTTL, and that
// AutoClose still works once everything drains.
func TestRetryNotLostOnWorkerIdle(t *testing.T) {
	done := make(chan struct{})
	var callbacks int32
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		atomic.AddInt32(&callbacks, 1)
		close(done)
	})
	doer := &statusDoer{
		statuses: []int{http.StatusTooManyRequests, http.StatusOK},
	}
	f := New(sh)
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.HttpClient = doer
	f.MaxRetries = 2
	f.RetryDelay = 60 * time.Millisecond
	f.MaxRetryDelay = 200 * time.Millisecond
	f.WorkerIdleTTL = 25 * time.Millisecond // shorter than the retry wait
	f.AutoClose = true
	q := f.Start()
	q.SendStringGet("http://example.com/a")
	waitForSignal(t, done, "handler callback", 5*time.Second)
	// AutoClose must eventually close the queue once the host goes idle.
	waitForSignal(t, q.Done(), "AutoClose", 5*time.Second)

	if got := atomic.LoadInt32(&callbacks); got != 1 {
		t.Errorf("expected 1 callback, got %d", got)
	}
	if doer.Calls() != 2 {
		t.Errorf("expected 2 HTTP requests, got %d", doer.Calls())
	}
}

// TestRestartNoRetryState verifies that restarting a Fetcher does not carry
// over retry state from the previous run.
func TestRestartNoRetryState(t *testing.T) {
	doer := &statusDoer{
		statuses: []int{http.StatusTooManyRequests, http.StatusOK},
	}
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {})
	f := New(sh)
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.HttpClient = doer
	f.MaxRetries = 1
	f.RetryDelay = 10 * time.Millisecond
	f.MaxRetryDelay = 50 * time.Millisecond

	q1 := f.Start()
	q1.SendStringGet("http://example.com/a")
	q1.Close()
	if doer.Calls() != 2 {
		t.Fatalf("first run: expected 2 requests, got %d", doer.Calls())
	}

	// Restart: the scripted doer counter resets logically; a fresh 429
	// should again be retried independently.
	doer.mu.Lock()
	atomic.StoreInt32(&doer.calls, 0)
	doer.mu.Unlock()
	q2 := f.Start()
	if len(f.hosts) != 0 {
		t.Errorf("expected clean hosts map after restart, got %d", len(f.hosts))
	}
	q2.SendStringGet("http://example.com/a")
	q2.Close()
	if doer.Calls() != 2 {
		t.Errorf("second run: expected 2 independent requests, got %d", doer.Calls())
	}
}

// TestRetryPreservesRequestData verifies that every retry carries the same
// headers, basic auth and cookies as the original command.
func TestRetryPreservesRequestData(t *testing.T) {
	done := make(chan struct{})
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) { close(done) })
	doer := &statusDoer{
		statuses: []int{http.StatusTooManyRequests, http.StatusTooManyRequests, http.StatusOK},
	}
	f := retryFetcher(sh, doer)
	f.RetryDelay = 5 * time.Millisecond
	q := f.Start()
	u := mustParseURL(t, "http://example.com/a")
	cmd := &headerAuthCookieCmd{Cmd: &Cmd{U: u, M: "GET"}}
	if err := q.Send(cmd); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, done, "handler callback", 5*time.Second)
	q.Close()

	if doer.Calls() != 3 {
		t.Fatalf("expected 3 requests, got %d", doer.Calls())
	}
	doer.mu.Lock()
	reqs := append([]*http.Request(nil), doer.gotReqs...)
	doer.mu.Unlock()
	for i, req := range reqs {
		if req.Method != "GET" {
			t.Errorf("request %d: expected GET, got %s", i, req.Method)
		}
		if got := req.Header.Get("X-Test"); got != "v" {
			t.Errorf("request %d: missing custom header, got %q", i, got)
		}
		if got := req.Header.Get("Authorization"); got == "" {
			t.Errorf("request %d: missing basic auth", i)
		}
		if len(req.Cookies()) != 1 || req.Cookies()[0].Name != "c" {
			t.Errorf("request %d: missing cookie, got %v", i, req.Cookies())
		}
	}
}

// TestCrawlDelayAndRetry verifies that before each same-host request,
// including retries, both the retry wait and the crawl delay are honored
// (concurrently, so the larger subsumes the smaller).
func TestCrawlDelayAndRetry(t *testing.T) {
	done := make(chan struct{})
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) { close(done) })
	doer := &statusDoer{
		statuses: []int{http.StatusTooManyRequests, http.StatusOK},
	}
	f := New(sh)
	f.DisablePoliteness = true
	f.CrawlDelay = 80 * time.Millisecond // larger than the retry delay
	f.HttpClient = doer
	f.MaxRetries = 1
	f.RetryDelay = 10 * time.Millisecond
	f.MaxRetryDelay = 100 * time.Millisecond
	start := time.Now()
	q := f.Start()
	q.SendStringGet("http://example.com/a")
	waitForSignal(t, done, "handler callback", 5*time.Second)
	elapsed := time.Since(start)
	q.Close()
	// The retry had to wait at least the crawl delay (~80ms), not just
	// the 10ms retry backoff.
	if elapsed < 70*time.Millisecond {
		t.Errorf("expected crawl delay to apply to the retry, elapsed %s", elapsed)
	}
}

// TestRetryConfigDefaults verifies the normalization done at Start.
func TestRetryConfigDefaults(t *testing.T) {
	// Retries disabled: fields are left untouched by Start.
	f := New(nopHandler)
	f.DisablePoliteness = true
	f.MaxRetries = 0
	f.RetryDelay = 0
	f.MaxRetryDelay = 0
	q := f.Start()
	q.Close()
	if f.RetryDelay != 0 || f.MaxRetryDelay != 0 {
		t.Errorf("disabled retries must not normalize delays, got %s/%s", f.RetryDelay, f.MaxRetryDelay)
	}

	// Retries enabled: non-positive delays get the documented defaults.
	f = New(nopHandler)
	f.MaxRetries = 2
	f.RetryDelay = 0
	f.MaxRetryDelay = -1
	q = f.Start()
	q.Close()
	if f.RetryDelay != DefaultRetryDelay {
		t.Errorf("expected default RetryDelay %s, got %s", DefaultRetryDelay, f.RetryDelay)
	}
	if f.MaxRetryDelay != DefaultMaxRetryDelay {
		t.Errorf("expected default MaxRetryDelay %s, got %s", DefaultMaxRetryDelay, f.MaxRetryDelay)
	}

	// Explicit positive values are preserved.
	f = New(nopHandler)
	f.MaxRetries = 1
	f.RetryDelay = 7 * time.Second
	f.MaxRetryDelay = 77 * time.Second
	q = f.Start()
	q.Close()
	if f.RetryDelay != 7*time.Second || f.MaxRetryDelay != 77*time.Second {
		t.Errorf("explicit delays not preserved: %s/%s", f.RetryDelay, f.MaxRetryDelay)
	}
}

// TestRobotsRequestNotRetried verifies that the internally generated
// robots.txt request, even on 429, is not retried.
func TestRobotsRequestNotRetried(t *testing.T) {
	var robotsCalls int32
	srv := httptestNewServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			atomic.AddInt32(&robotsCalls, 1)
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	defer srv.Close()

	done := make(chan struct{})
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) { close(done) })
	f := New(sh)
	f.CrawlDelay = 0
	f.MaxRetries = 3
	f.RetryDelay = 10 * time.Millisecond
	f.MaxRetryDelay = 50 * time.Millisecond
	q := f.Start()
	q.SendStringGet(srv.URL + "/a")
	waitForSignal(t, done, "handler callback", 5*time.Second)
	q.Close()
	if got := atomic.LoadInt32(&robotsCalls); got != 1 {
		t.Errorf("expected robots.txt to be requested exactly once, got %d", got)
	}
}

// TestRetryAfterHTTPDateEndToEnd verifies that an HTTP-date Retry-After
// header is interpreted as an absolute time and replaces the exponential
// backoff for that attempt. HTTP dates have whole-second resolution.
func TestRetryAfterHTTPDateEndToEnd(t *testing.T) {
	date := time.Now().Add(time.Second).UTC().Format(http.TimeFormat)
	done := make(chan struct{})
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) { close(done) })
	doer := &statusDoer{
		statuses: []int{http.StatusServiceUnavailable, http.StatusOK},
		headers:  []http.Header{{"Retry-After": {date}}, nil},
	}
	f := retryFetcher(sh, doer)
	f.RetryDelay = 10 * time.Second // a 10s backoff would blow the timeout if the date were ignored
	f.MaxRetryDelay = 30 * time.Second
	start := time.Now()
	q := f.Start()
	if _, err := q.SendStringGet("http://example.com/a"); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, done, "handler callback", 5*time.Second)
	elapsed := time.Since(start)
	q.Close()

	if doer.Calls() != 2 {
		t.Fatalf("expected 2 requests, got %d", doer.Calls())
	}
	// The date is at most about one whole second in the future, so the
	// retry must complete well before the 10s exponential backoff.
	if elapsed >= 2*time.Second {
		t.Errorf("expected the HTTP-date wait (under ~1s) to replace the 10s backoff, took %s", elapsed)
	}
}

// TestCancelInterruptsRetryAndQueuedCommand verifies that Cancel promptly
// interrupts a retry wait, starts no further retry, and dispatches no
// callback - neither for the command still retrying nor for the command
// queued behind it on the same host.
func TestCancelInterruptsRetryAndQueuedCommand(t *testing.T) {
	firstAttempt := make(chan struct{})
	var requests int32
	var callbacks int32
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) {
		atomic.AddInt32(&callbacks, 1)
	})
	doer := doerFunc(func(req *http.Request) (*http.Response, error) {
		if atomic.AddInt32(&requests, 1) == 1 {
			close(firstAttempt)
		}
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Request:    req,
			Header:     http.Header{},
			Body:       ioutil.NopCloser(strings.NewReader("")),
		}, nil
	})
	f := New(sh)
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	f.HttpClient = doer
	f.MaxRetries = 5
	f.RetryDelay = 10 * time.Second // long wait that Cancel must interrupt
	f.MaxRetryDelay = time.Minute
	q := f.Start()
	if _, err := q.SendStringGet("http://example.com/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.SendStringGet("http://example.com/b"); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, firstAttempt, "first HTTP attempt", 5*time.Second)

	start := time.Now()
	if err := q.Cancel(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Cancel should interrupt the retry wait promptly, took %s", elapsed)
	}

	if got := atomic.LoadInt32(&callbacks); got != 0 {
		t.Errorf("expected no callbacks after cancellation, got %d", got)
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Errorf("expected no new HTTP attempt after cancellation, got %d", got)
	}
}

// TestRetryAfterPastDateEndToEnd verifies that a past HTTP date means an
// immediate retry.
func TestRetryAfterPastDateEndToEnd(t *testing.T) {
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	done := make(chan struct{})
	sh := HandlerFunc(func(ctx *Context, res *http.Response, err error) { close(done) })
	doer := &statusDoer{
		statuses: []int{http.StatusTooManyRequests, http.StatusOK},
		headers:  []http.Header{{"Retry-After": {past}}, nil},
	}
	f := retryFetcher(sh, doer)
	f.RetryDelay = 2 * time.Second
	start := time.Now()
	q := f.Start()
	if _, err := q.SendStringGet("http://example.com/a"); err != nil {
		t.Fatal(err)
	}
	waitForSignal(t, done, "handler callback", 5*time.Second)
	elapsed := time.Since(start)
	q.Close()
	if elapsed > 100*time.Millisecond {
		t.Errorf("past HTTP date should mean an immediate retry, took %s", elapsed)
	}
	if doer.Calls() != 2 {
		t.Errorf("expected 2 requests, got %d", doer.Calls())
	}
}
