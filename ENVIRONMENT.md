# Native Windows environment

Validated on Windows with Go 1.26.5 windows/amd64 and Git 2.55.0.windows.2.
The module and dependency versions remain as declared in go.mod and go.sum.
No database, browser, external test service, or Docker is required for the test suite.
Tests create local HTTP servers; local loopback listening must be permitted.

Run from the repository root in PowerShell or Git Bash:

```text
go mod download
go test ./...
```

Set GOCACHE and GOMODCACHE to persistent directories outside the repository if desired.
An HTTP proxy may be needed for dependency downloads on some networks. Configure it in
the local process; do not commit proxy credentials, API tokens, or personal CLI settings.
The race detector additionally requires an appropriate native C toolchain and CGO.
