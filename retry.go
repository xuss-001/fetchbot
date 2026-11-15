// Copyright 2014 Martin Angers and Contributors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fetchbot

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultRetryDelay is the default initial wait time before retrying a
	// command that received a 429 or 503 response, when RetryDelay is not
	// explicitly set on the Fetcher.
	DefaultRetryDelay = 1 * time.Second

	// DefaultMaxRetryDelay is the default upper limit of the wait time
	// computed by the retry rules, when MaxRetryDelay is not explicitly set
	// on the Fetcher.
	DefaultMaxRetryDelay = 30 * time.Second
)

// canRetryCommand reports whether the retry machinery applies to this
// command. Only user-enqueued GET or HEAD commands that do not provide a
// request body (neither ReaderProvider nor ValuesProvider) are eligible.
// The internal robots.txt command, any other HTTP method and commands with
// a body are never retried.
func canRetryCommand(cmd Command) bool {
	if _, ok := cmd.(robotCommand); ok {
		return false
	}
	if m := cmd.Method(); m != "GET" && m != "HEAD" {
		return false
	}
	if _, ok := cmd.(ReaderProvider); ok {
		return false
	}
	if _, ok := cmd.(ValuesProvider); ok {
		return false
	}
	return true
}

// isRetryableStatus reports whether the response status code triggers the
// retry rules. Only 429 (Too Many Requests) and 503 (Service Unavailable)
// do.
func isRetryableStatus(res *http.Response) bool {
	return res != nil && (res.StatusCode == http.StatusTooManyRequests ||
		res.StatusCode == http.StatusServiceUnavailable)
}

// fetch executes a command, retrying it according to the Fetcher's retry
// configuration when eligible responses return a 429 or 503 status. It is
// executed entirely within the host's worker goroutine, so retries never
// re-enqueue the command through the public Queue and cannot overtake
// another command for the same host.
//
// crawlDelay is the host's current per-request interval (from the
// robots.txt policy or the Fetcher's CrawlDelay). Before every next request
// to the host, including a retry, both the retry-derived wait and the crawl
// delay must have elapsed since the previous request completed. They run
// concurrently, so the larger of the two subsumes the other.
//
// The returned postWait duration is the wait time computed by the retry
// rules on the last 429/503 response (zero otherwise). It must still
// constrain the next command on this host even if the command has exhausted
// its retries. The aborted flag is true when the Queue was cancelled while
// the command was waiting for a retry, in which case no Handler callback
// must be dispatched.
func (f *Fetcher) fetch(cmd Command, crawlDelay time.Duration) (res *http.Response, err error, postWait time.Duration, aborted bool) {
	eligible := f.MaxRetries > 0 && canRetryCommand(cmd)
	attempts := 0
	backoff := f.RetryDelay

	for {
		// A cancellation that happens while no request is in flight
		// prevents the next attempt and any callback.
		if f.isCancelled() {
			return nil, nil, 0, true
		}
		res, err = f.doRequest(cmd)
		if err != nil {
			// Network errors are never retried, and they do not impose
			// a retry-derived wait on subsequent commands.
			return nil, err, 0, false
		}
		if !eligible || !isRetryableStatus(res) || attempts >= f.MaxRetries {
			// This is the terminal outcome for the command. If the
			// Queue was cancelled while the request was in flight,
			// drain it without a callback.
			if f.isCancelled() {
				closeBody(res)
				return nil, nil, 0, true
			}
			return res, nil, postWait, false
		}

		// The response is an intermediate, retryable one. It must never
		// reach the Handler, and its body must be closed before waiting.
		retry := retryWait(res.Header, backoff, f.MaxRetryDelay)
		postWait = retry
		closeBody(res)
		res = nil

		// Both the retry wait and the host crawl interval started when the
		// previous request completed, so waiting for the larger duration
		// satisfies both.
		wait := retry
		if crawlDelay > wait {
			wait = crawlDelay
		}
		if !f.sleepCancelled(wait) {
			return nil, nil, 0, true
		}

		attempts++
		backoff = doubleBackoff(backoff, f.MaxRetryDelay)
	}
}

// retryWait computes the wait time before the next attempt. A valid
// Retry-After header replaces the exponential backoff value for this
// attempt; an invalid or missing header falls back to backoff. Both kinds
// of values are capped at maxDelay.
func retryWait(h http.Header, backoff, maxDelay time.Duration) time.Duration {
	wait := backoff
	if ra, ok := parseRetryAfter(h.Get("Retry-After")); ok {
		wait = ra
	}
	if wait > maxDelay {
		wait = maxDelay
	}
	return wait
}

// parseRetryAfter parses a Retry-After header value as defined by RFC 7231.
// The value is either a non-negative decimal number of seconds, or an HTTP
// date in one of the formats accepted by http.ParseTime. Leading and
// trailing whitespace is trimmed. Dates in the past yield a zero duration.
// The boolean result is false for invalid values, in which case the caller
// must fall back to the default backoff.
func parseRetryAfter(val string) (time.Duration, bool) {
	val = strings.TrimSpace(val)
	if val == "" {
		return 0, false
	}
	if secs, err := strconv.ParseInt(val, 10, 64); err == nil {
		if secs < 0 {
			return 0, false
		}
		// Clamp values that cannot be represented as a Duration so that
		// retryWait can apply the MaxRetryDelay cap instead of wrapping
		// around to a negative duration.
		if secs > int64(math.MaxInt64/int64(time.Second)) {
			return time.Duration(math.MaxInt64), true
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(val); err == nil {
		now := time.Now()
		if !t.After(now) {
			return 0, true
		}
		d := t.Sub(now)
		if d <= 0 {
			// The date is in the future but the difference overflows a
			// Duration; clamp it so MaxRetryDelay can cap it.
			return time.Duration(math.MaxInt64), true
		}
		return d, true
	}
	return 0, false
}

// doubleBackoff doubles the backoff duration without ever overflowing and
// caps the result at maxDelay.
func doubleBackoff(d, maxDelay time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	if d > maxDelay/2 {
		// Doubling would either overflow or exceed the cap.
		return maxDelay
	}
	d *= 2
	if d > maxDelay {
		return maxDelay
	}
	return d
}

// closeBody closes the response body, ignoring a nil body or close error.
func closeBody(res *http.Response) {
	if res != nil && res.Body != nil {
		res.Body.Close()
	}
}

// isCancelled reports whether the Queue has been cancelled.
func (f *Fetcher) isCancelled() bool {
	select {
	case <-f.q.cancelled:
		return true
	default:
		return false
	}
}

// sleepCancelled waits for the given duration, but returns early (with
// false) if the Queue gets cancelled during the wait. It returns true when
// the full duration elapsed without cancellation. A non-positive duration
// returns immediately.
func (f *Fetcher) sleepCancelled(d time.Duration) bool {
	if d <= 0 {
		return !f.isCancelled()
	}
	select {
	case <-f.q.cancelled:
		return false
	case <-time.After(d):
		return true
	}
}
