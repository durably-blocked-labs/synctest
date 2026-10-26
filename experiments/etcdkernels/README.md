# Historical etcd bug kernels

The three `.go` files here are byte-for-byte copies of reduced kernels from
this repository's earlier GoBench evaluation. `sources.json` records their Git
source paths and SHA-256 hashes. They are small reproductions of historical
bugs, not full etcd systems or tests of current etcd. We keep the kernel source
unchanged so a new harness cannot silently change the earlier workload.

The runnable harness currently covers **etcd #6873**. An update worker takes
`mu` while processing a queued notification. `stop` can take the same mutex,
close the update channel, and wait for that worker to finish before releasing
`mu`. If the worker is already queued, each waits for the other. The upstream
fix releases the mutex before waiting for completion. This is a historical
bug, not a new upstream finding.

Run a bounded search with a qualified Bubbles Go toolchain and a fresh output
directory:

```sh
python3 scripts/etcd-kernel-search.py --go /absolute/path/to/goroot/bin/go \
  --output /absolute/path/to/new-results --strategy dfs --max-runs 100
```

`--strategy pct` samples seeds (`--seed` and `--depth`); `random` and `fifo`
are also available. The driver compiles one test worker, runs each schedule in
a separate process, saves every chosen runnable set and result, and replays
each suspected deadlock in another process. A deadlock finding requires both
a bounded test timeout and the expected `coalesce` mutex wait and `stop`
channel wait in the stack dump. A timeout by itself is an incomplete trial.
The worker records decisions in a preallocated memory mapping so file I/O
cannot interrupt the scheduler's choice hook.

The replay confirms the saved **hook choices and failure cycle**. A timed-out
worker cannot return a complete runtime trace, so this is not a successful
`synctest.StrictReplay` result. DFS exhaustion means all branches exposed by
this kernel's current decision hook were visited within the stated bound; it
does not cover unexposed Go preemptions, other inputs, or full etcd.

The #7443 and #7902 kernels are retained for provenance and future harness
work. The archived #7443 source header mentions a close race, but the
historical failure involves balancer notification and lock ordering; read the
actual functions before describing it. The old #7492 reduction is excluded:
it has two token assigners while the original GoBench construction has three,
so a negative result from it would not qualify the historical bug.
