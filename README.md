# fetchbot [![build status](https://secure.travis-ci.org/PuerkitoBio/fetchbot.svg)](http://travis-ci.org/PuerkitoBio/fetchbot) [![Go Reference](https://pkg.go.dev/badge/github.com/PuerkitoBio/fetchbot.svg)](https://pkg.go.dev/github.com/PuerkitoBio/fetchbot)

Package fetchbot provides a simple and flexible web crawler that follows the robots.txt
policies and crawl delays.

It is very much a rewrite of [gocrawl](https://github.com/PuerkitoBio/gocrawl) with a
simpler API, less features built-in, but at the same time more flexibility. As for Go
itself, sometimes less is more!

## Installation

To install, simply run in a terminal:

    go get github.com/PuerkitoBio/fetchbot

The package has a single external dependency, [robotstxt](https://github.com/temoto/robotstxt). It also integrates code from the [iq package](https://github.com/kylelemons/iq).

The [API documentation is available on godoc.org](http://godoc.org/github.com/PuerkitoBio/fetchbot).

## Changes

* Unreleased: add per-command automatic retries on `429` and `503` responses, configured via the new `MaxRetries`, `RetryDelay` and `MaxRetryDelay` Fetcher fields, with `Retry-After` header support (see [Retries](#retries)).
* 2019-09-11 (v1.2.0): update robotstxt dependency (import path/repo URL has changed, issue #31, thanks to [@michael-stevens][michael-stevens] for raising the issue).
* 2017-09-04 (v1.1.1): fix a goroutine leak when cancelling a Queue (issue #26, thanks to [@ryu-koui][ryu] for raising the issue).
* 2017-07-06 (v1.1.0): add `Queue.Done` to get the done channel on the queue, allowing to wait in a `select` statement (thanks to [@DennisDenuto][denuto]).
* 2015-07-25 (v1.0.0) : add `Cancel` method on the `Queue`, to close and drain without requesting any pending commands, unlike `Close` that waits for all pending commands to be processed (thanks to [@buro9][buro9] for the feature request).
* 2015-07-24 : add `HandlerCmd` and call the Command's `Handler` function if it implements the `Handler` interface, bypassing the `Fetcher`'s handler. Support a `Custom` matcher on the `Mux`, using a predicate. (thanks to [@mmcdole][mmcdole] for the feature requests).
* 2015-06-18 : add `Scheme` criteria on the muxer (thanks to [@buro9][buro9]).
* 2015-06-10 : add `DisablePoliteness` field on the `Fetcher` to optionally bypass robots.txt checks (thanks to [@oli-g][oli]).
* 2014-07-04 : change the type of Fetcher.HttpClient from `*http.Client` to the `Doer` interface. Low chance of breaking existing code, but it's a possibility if someone used the fetcher's client to run other requests (e.g. `f.HttpClient.Get(...)`).

## Usage

The following example (taken from /example/short/main.go) shows how to create and
start a Fetcher, one way to send commands, and how to stop the fetcher once all
commands have been handled.

```go
package main

import (
	"fmt"
	"net/http"

	"github.com/PuerkitoBio/fetchbot"
)

func main() {
	f := fetchbot.New(fetchbot.HandlerFunc(handler))
	queue := f.Start()
	queue.SendStringHead("http://google.com", "http://golang.org", "http://golang.org/doc")
	queue.Close()
}

func handler(ctx *fetchbot.Context, res *http.Response, err error) {
	if err != nil {
		fmt.Printf("error: %s\n", err)
		return
	}
	fmt.Printf("[%d] %s %s\n", res.StatusCode, ctx.Cmd.Method(), ctx.Cmd.URL())
}
```

A more complex and complete example can be found in the repository, at /example/full/.

### Retries

Some hosts briefly return `429 Too Many Requests` or `503 Service Unavailable`
under load. Instead of letting the business callback re-enqueue the command
(which would scramble the per-host request order) and instead of letting one
rate-limited host stall every other host, the Fetcher can retry the affected
commands internally. Three fields control this behavior and must be set before
`Start` (they are not safe to change while the Fetcher is running):

* `MaxRetries`: the maximum number of retries *in addition to* the initial
request, counted independently for each command. A value of `0` or less (the
default) keeps the original, non-retrying behavior, so existing code is
unaffected.
* `RetryDelay`: the initial wait before the first retry. It defaults to 1
second. On consecutive `429`/`503` responses the wait doubles (1s, 2s, 4s,
...) without ever overflowing.
* `MaxRetryDelay`: the upper limit of a retry wait, including waits derived
from a `Retry-After` header. It defaults to 30 seconds.

When retries are enabled, a `RetryDelay` or `MaxRetryDelay` that is zero or
negative at `Start` time is replaced by its default. `MaxRetryDelay` only
caps the retry rules; it never limits the host request interval
(`CrawlDelay` or the robots.txt crawl delay).

Only user-enqueued `GET` or `HEAD` commands that provide no request body
(neither `ReaderProvider` nor `ValuesProvider`) are retried. Other HTTP
methods, commands with a body, the internal robots.txt request and network
errors are never retried. Intermediate `429`/`503` responses never reach the
`Handler`: their bodies are closed before the wait, and a single final
response or error is dispatched exactly once through the usual command-
specific or global Handler rules. Retries reuse the original command, so
custom headers, basic authentication and cookies are preserved, and the
retry is never implemented by re-sending the command to the public Queue.

If the response carries a `Retry-After` header, a valid value replaces the
exponential wait for that attempt: a non-negative decimal integer is
interpreted as seconds and an HTTP date (in any format accepted by
`http.ParseTime`) as an absolute time, with dates in the past meaning zero
wait. Leading and trailing whitespace is ignored, invalid values fall back
to the exponential wait, and oversized values are capped at
`MaxRetryDelay`.

Retries run inside the host's dedicated worker, so later commands for the
same host cannot overtake a command that is still retrying, while other
hosts keep processing independently. Before the next same-host request -
including a retry - both the retry wait and the host request interval must
have elapsed; both start when the previous request completes, so they run
concurrently and the larger one subsumes the other rather than being added.
Even when a command has exhausted its retries, the wait computed from its
last `429`/`503` still delays subsequent commands on that host. When the
Queue has no more work, it does not stay open just to fully elapse that
wait.

`Queue.Close` waits for accepted commands and their retries to finish.
`Queue.Cancel` interrupts retry waits promptly, starts no new retry and
dispatches no callback for commands that have not completed; in-flight HTTP
requests and running callbacks finish as before. Commands waiting between
retries are not lost to `WorkerIdleTTL` expiry or `AutoClose`, and
restarting a Fetcher never inherits the previous Queue's retry state.

A runnable, fully offline demo that starts a local server returning
`429`/`503` before `200` lives in /example/retry/.

```go
f := fetchbot.New(fetchbot.HandlerFunc(handler))
f.MaxRetries = 3
f.RetryDelay = 500 * time.Millisecond
f.MaxRetryDelay = 30 * time.Second
queue := f.Start()
queue.SendStringGet("https://example.com/rate-limited")
queue.Close()
```

### Fetcher

Basically, a **Fetcher** is an instance of a web crawler, independent of other Fetchers.
It receives Commands via the **Queue**, executes the requests, and calls a **Handler** to
process the responses. A **Command** is an interface that tells the Fetcher which URL to
fetch, and which HTTP method to use (i.e. "GET", "HEAD", ...).

A call to Fetcher.Start() returns the Queue associated with this Fetcher. This is the
thread-safe object that can be used to send commands, or to stop the crawler.

Both the Command and the Handler are interfaces, and may be implemented in various ways.
They are defined like so:

```go
type Command interface {
	URL() *url.URL
	Method() string
}
type Handler interface {
	Handle(*Context, *http.Response, error)
}
```

A **Context** is a struct that holds the Command and the Queue, so that the Handler always
knows which Command initiated this call, and has a handle to the Queue.

A Handler is similar to the net/http Handler, and middleware-style combinations can
be built on top of it. A HandlerFunc type is provided so that simple functions
with the right signature can be used as Handlers (like net/http.HandlerFunc), and there
is also a multiplexer Mux that can be used to dispatch calls to different Handlers
based on some criteria.

### Command-related Interfaces

The Fetcher recognizes a number of interfaces that the Command may implement, for
more advanced needs.

* `BasicAuthProvider`: Implement this interface to specify the basic authentication
credentials to set on the request.

* `CookiesProvider`: If the Command implements this interface, the provided Cookies
will be set on the request.

* `HeaderProvider`: Implement this interface to specify the headers to set on the
request.

* `ReaderProvider`: Implement this interface to set the body of the request, via
an `io.Reader`.

* `ValuesProvider`: Implement this interface to set the body of the request, as
form-encoded values. If the Content-Type is not specifically set via a `HeaderProvider`,
it is set to "application/x-www-form-urlencoded". `ReaderProvider` and `ValuesProvider`
should be mutually exclusive as they both set the body of the request. If both are
implemented, the `ReaderProvider` interface is used.

* `Handler`: Implement this interface if the Command's response should be handled
by a specific callback function. By default, the response is handled by the Fetcher's
Handler, but if the Command implements this, this handler function takes precedence
and the Fetcher's Handler is ignored.

Since the Command is an interface, it can be a custom struct that holds additional
information, such as an ID for the URL (e.g. from a database), or a depth counter
so that the crawling stops at a certain depth, etc. For basic commands that don't
require additional information, the package provides the Cmd struct that implements
the Command interface. This is the Command implementation used when using the
various Queue.SendString\* methods.

There is also a convenience `HandlerCmd` struct for the commands that should be handled
by a specific callback function. It is a Command with a Handler interface implementation.

### Fetcher Options

The Fetcher has a number of fields that provide further customization:

* HttpClient : By default, the Fetcher uses the net/http default Client to make requests. A
different client can be set on the Fetcher.HttpClient field.

* CrawlDelay : That value is used only if there is no delay specified
by the robots.txt of a given host.

* UserAgent : Sets the user agent string to use for the requests and to validate
against the robots.txt entries.

* WorkerIdleTTL : Sets the duration that a worker goroutine can wait without receiving
new commands to fetch. If the idle time-to-live is reached, the worker goroutine
is stopped and its resources are released. This can be especially useful for
long-running crawlers.

* AutoClose : If true, closes the queue automatically once the number of active hosts
reach 0.

* DisablePoliteness : If true, ignores the robots.txt policies of the hosts.

* MaxRetries, RetryDelay, MaxRetryDelay : Control automatic retries of
eligible GET/HEAD commands that receive a 429 or 503 response. See the
[Retries](#retries) section for the full semantics. Retries are disabled
when MaxRetries is zero or negative (the default).

What fetchbot doesn't do - especially compared to gocrawl - is that it doesn't
keep track of already visited URLs, and it doesn't normalize the URLs. This is outside
the scope of this package - all commands sent on the Queue will be fetched.
Normalization can easily be done (e.g. using [purell](https://github.com/PuerkitoBio/purell)) before sending the Command to the Fetcher.
How to keep track of visited URLs depends on the use-case of the specific crawler,
but for an example, see /example/full/main.go.

## License

The [BSD 3-Clause license](http://opensource.org/licenses/BSD-3-Clause), the same as
the Go language. The iq package source code is under the CDDL-1.0 license (details in
the source file).

[oli]: https://github.com/oli-g
[buro9]: https://github.com/buro9
[mmcdole]: https://github.com/mmcdole
[denuto]: https://github.com/DennisDenuto
[ryu]: https://github.com/ryu-koui
[michael-stevens]: https://github.com/michael-stevens

