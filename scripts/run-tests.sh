#!/bin/sh
# Explicit module graph; separate etcd external-I/O baseline is opt-in.
# Invoke under an external process-group watchdog; Go test timeouts are separate.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
GO_BIN=${GO_BIN:-"$ROOT/go/bin/go"}
export GOROOT=$(CDPATH= cd -- "$(dirname -- "$GO_BIN")/.." && pwd)
export GOTOOLCHAIN=local GOWORK=off
export GODEBUG=asyncpreemptoff=1
cd "$ROOT"
case "${1:-maintained}" in
  maintained) "$GO_BIN" test -p=4 -count=1 -timeout=90s ./explorer/... ./orchestrator/... ./rafttest/... ./campaign/... ./experiments/runtime/... ./experiments/ragate/... ./experiments/raft/... ;;
  ragate) "$GO_BIN" test -p=4 -count=1 -timeout=45s ./experiments/ragate/... ;;
  resourcegc-smoke) "$GO_BIN" test -p=4 -count=1 -timeout=45s -run '^(TestLifecycleErrorPaths|TestCleanupSkipsOwnedResource|TestStaleCloseAfterCommitSucceeds|TestRunApplicationReturnsCompletedObservation|TestObservationOverflowIsReported)$' ./experiments/resourcegc/... ;;
  raft) python3 scripts/verify-dependencies.py --only raft; "$GO_BIN" test -p=4 -count=1 -timeout=45s ./experiments/raft/... ;;
  etcd)
    # A profile is explicit provenance. Validate it before dependency inspection.
    case "${ETCD_PROFILE:-}" in
      stock|custom-http2legacy|custom-untagged) ;;
      *) echo 'set ETCD_PROFILE=stock|custom-http2legacy|custom-untagged (untagged custom compatibility is not guaranteed)' >&2; exit 2 ;;
    esac
    python3 scripts/verify-dependencies.py --only etcd
    cd experiments/etcd
    case "$ETCD_PROFILE" in
      custom-http2legacy) "$GO_BIN" test -p=4 -tags=http2legacy -count=1 -timeout=90s ./... ;;
      stock|custom-untagged) "$GO_BIN" test -p=4 -count=1 -timeout=90s ./... ;;
    esac
    ;;
  *) echo 'usage: run-tests.sh maintained|ragate|resourcegc-smoke|raft|etcd' >&2; exit 2 ;;
esac
