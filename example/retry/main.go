// Example retry demonstrates the Fetcher's automatic retry of GET/HEAD
// commands that receive a 429 or 503 response. It starts a local test
// server that answers the first two requests with 429 (the second one
// carrying a Retry-After header) and the third with 200, then prints the
// final successful response. No external network access is required.
package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/PuerkitoBio/fetchbot"
)

func main() {
	// Local test server: the first two calls fail with 429 (simulating a
	// rate-limited host), subsequent calls succeed with 200.
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		switch n {
		case 1:
			// No Retry-After: the Fetcher uses its exponential RetryDelay.
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprintln(w, "rate limited")
		case 2:
			// Explicit Retry-After: the header value replaces the
			// exponential wait for this attempt.
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintln(w, "temporarily unavailable")
		default:
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "hello after retries")
		}
	}))
	defer srv.Close()

	f := fetchbot.New(fetchbot.HandlerFunc(handler))
	// Keep the example fast and fully local.
	f.DisablePoliteness = true
	f.CrawlDelay = 0
	// Retry configuration: up to 3 retries per command, starting at 250ms
	// and doubling up to a 5s ceiling.
	f.MaxRetries = 3
	f.RetryDelay = 250 * time.Millisecond
	f.MaxRetryDelay = 5 * time.Second

	queue := f.Start()
	if _, err := queue.SendStringGet(srv.URL + "/resource"); err != nil {
		log.Fatal(err)
	}
	// Close waits for the accepted command and all of its retries.
	queue.Close()

	fmt.Printf("server received %d requests in total\n", atomic.LoadInt32(&calls))
}

func handler(ctx *fetchbot.Context, res *http.Response, err error) {
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Printf("read body: %s\n", err)
		return
	}
	fmt.Printf("final response: [%d] %s body=%q\n", res.StatusCode, ctx.Cmd.URL(), string(body))
}
