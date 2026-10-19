# Full embedded etcd: external baseline

Run explicitly with `make test-etcd ETCD_PROFILE=custom-http2legacy` and a
verified custom toolchain, or `ETCD_PROFILE=stock` with explicit stock
`GO_ROOT`/`GO_BIN` >=1.26. This nested Go module is intentionally not
silently added to root `go test ./...`: etcd 3.6.15's dependency graph is separate
from historical kernels and the maintained scheduler library. The runner checks
local pins before executing. All five etcd module replacements use root `etcd/`,
the public durably-blocked-labs fork at
`a3346427fbe6e456e9b6d6e429346481c6be7c71` (annotated `v3.6.15`), not main.
Etcd's separate `go.etcd.io/raft/v3 v3.6.0` is NOT HashiCorp Raft.

The baseline starts three full embedded servers with unique per-node directories,
peer/client URLs, instance observations and real clientv3 API calls. A bounded
sequential Put/CAS/Get oracle checks values and revisions. It closes one follower,
writes through the remaining quorum, and reopens the same follower directory;
member identity, per-instance applied index and linearizable reads are checked.
Clients and servers close on all exits. Timeout/error responses are inconclusive,
not assumed failed/no-effect writes. A host test watchdog bounds infrastructure.

**Real loopback sockets, HTTP/gRPC, OS scheduling, WAL and bbolt files are used.**
This is not deterministic Bubbles simulation, abrupt-crash recovery, a complete
linearizability campaign or an independent-machine model. Graceful Close may
flush state. Per-instance MemberID/Leader/AppliedIndex/CommittedIndex are used;
process-global expvar/Prometheus telemetry is not a node-local oracle. Ephemeral
port reservation followed by bind has an unavoidable race; start failures are
infrastructure outcomes, never silently retried as target semantics.

## Controlled integration: unsupported, separate capability seam

`ControlledIntegrationStatus()` explicitly reports unsupported with required
boundaries. A future adapter must preserve full client/peer codecs and server
paths while controlling listeners/dialers (including snapshots/reconnects),
storage completion, time/random inputs, shared globals and cross-bubble
ownership. Strict replay and joined reset must qualify it independently. Merely
replacing Raft Step or the client dialer does not fulfill this boundary. The
external baseline never advertises this capability.

## Named HTTP2 compatibility profiles

On the older modified Go1.27-dev standard library, untagged pinned x/net0.58.0
selects its new wrapping implementation but net/http lacks the required
ServeConnFunc handshake. This produces an infrastructure panic, not an etcd bug.
`custom-http2legacy` explicitly passes `-tags=http2legacy`, selecting upstream
x/net's full legacy HTTP2 implementation; it does **not** disable HTTP2/gRPC,
change versions or patch dependency sources. `stock` passes no tags on stock
Go >=1.26. `custom-untagged` retains the untagged compatibility probe, not a
promise it works. The runner requires a named profile and never silently alters
global GOFLAGS. Both successful profiles remain external loopback/files baselines.
