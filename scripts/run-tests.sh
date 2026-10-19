#!/bin/sh
# Explicit module graph. Never discovers/executes the historical bug corpus implicitly.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
GO_BIN=${GO_BIN:-"$ROOT/go/bin/go"}
export GOROOT=$(CDPATH= cd -- "$(dirname -- "$GO_BIN")/.." && pwd)
export GOTOOLCHAIN=local GOWORK=off
export GODEBUG=asyncpreemptoff=1
cd "$ROOT"
case "${1:-maintained}" in
  maintained) "$GO_BIN" test -count=1 -timeout=90s ./explorer/... ./orchestrator/... ./rafttest/... ./campaign/... ./experiments/runtime/... ./experiments/raft/... ;;
  raft) python3 scripts/verify-dependencies.py --only raft; "$GO_BIN" test -count=1 -timeout=45s ./experiments/raft/... ;;
  etcd)
    python3 scripts/verify-dependencies.py --only etcd
    cd experiments/etcd
    # A profile is explicit provenance. Do not silently add global GOFLAGS.
    case "${ETCD_PROFILE:-}" in
      custom-http2legacy) "$GO_BIN" test -tags=http2legacy -count=1 -timeout=90s ./... ;;
      stock|custom-untagged) "$GO_BIN" test -count=1 -timeout=90s ./... ;;
      *) echo 'set ETCD_PROFILE=stock|custom-http2legacy|custom-untagged (untagged custom compatibility is not guaranteed)' >&2; exit 2 ;;
    esac
    ;;
  legacy-regressions)
    cd archive/legacy/bugs
    # Protect store/client/transport/trace/seed/budget characterization, but not bug searches.
    "$GO_BIN" test -count=1 -timeout=60s ./quorum-read-repair -run '^(TestBenchmarkOutcome|TestCompleteQuorumBugTrace|TestBenchSeed|TestBenchMaxRuns|TestClient|TestNewClient|TestCorrectMerge|TestBuggyRepairMerge|TestReplicaFIFO|TestMessageInbox|TestTransport)'
    ;;
  legacy-raft)
    # Historical external tests still import the current root adapter/orchestrator.
    # Original local transport is preserved, not the imported adapter under test.
    # Pinned Raft lacks Config.SkipTermCheck: compilation is currently blocked.
    test "$#" -eq 2 || { echo 'usage: run-tests.sh legacy-raft test-pattern (known compile incompatibility)' >&2; exit 2; }
    cd archive/legacy/rafttest
    "$GO_BIN" test -count=1 -timeout=60s . -run "$2"
    ;;
  legacy)
    # Explicit opt-in only; callers must name one package and a bounded test expression.
    test "$#" -eq 3 || { echo 'usage: run-tests.sh legacy ./gobench/etcd7492 TestName' >&2; exit 2; }
    cd archive/legacy/bugs
    "$GO_BIN" test -count=1 -timeout=60s "$2" -run "$3"
    ;;
  *) echo 'usage: run-tests.sh maintained|raft|etcd|legacy-regressions|legacy-raft test-pattern|legacy package test-pattern' >&2; exit 2 ;;
esac
