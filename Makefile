# Explicit package/module boundaries. No target deletes experiment data.
GO_ROOT ?= $(CURDIR)/go
GO_BIN ?= $(GO_ROOT)/bin/go
PKG ?= experiments/raft
TEST ?= TestThreeVoterMemoryServiceSingleBubble
TEST_PATTERN ?= .
WATCHDOG ?= 60
ETCD_PROFILE ?=
# Each saved run must explicitly own a NEW directory.
RUN_DIR ?=
GO_SOURCE ?= $(CURDIR)/go
BOOTSTRAP ?=
TOOLCHAIN_DIR ?=
BUILD_JOBS ?= 4
BUILD_WATCHDOG ?= 600
BUBBLEGUN_BUILD_DIR ?=

.PHONY: help build-go go-version test test-all test-det test-one determinism \
        test-ragate test-resourcegc-smoke test-raft test-etcd test-entrypoints test-campaign-entrypoints bubblegun-build clean
help:
	@echo 'test PKG=package TEST_PATTERN=pattern RUN_DIR=/new/run: saved, bounded test'
	@echo 'test-all: root library + runtime, RA protocol and single-bubble Raft tests'
	@echo 'test-ragate: synthetic RA gate protocol unit tests; no distributed adapter'
	@echo 'test-resourcegc-smoke RUN_DIR=/new/run: bounded application/observation smoke'
	@echo 'build-go BOOTSTRAP=/bootstrap/go TOOLCHAIN_DIR=/new/toolchain: bounded build/copy/seal/smoke'
	@echo 'test-raft: single-bubble three-voter memory service and small-history oracle'
	@echo 'test-etcd ETCD_PROFILE=stock|custom-http2legacy|custom-untagged: external I/O'
	@echo 'bubblegun-build BUBBLEGUN_BUILD_DIR=/new/directory: bounded Bubble Gun build'
	@echo 'Group test targets require an external process-group watchdog; see README.'

build-go:
	@test -n "$(BOOTSTRAP)" -a -n "$(TOOLCHAIN_DIR)" || { echo 'set BOOTSTRAP and a new TOOLCHAIN_DIR'; exit 2; }
	python3 scripts/build-toolchain.py --source "$(GO_SOURCE)" --bootstrap "$(BOOTSTRAP)" --output "$(TOOLCHAIN_DIR)" --jobs "$(BUILD_JOBS)" --timeout "$(BUILD_WATCHDOG)"

go-version:
	GOROOT="$(GO_ROOT)" GOTOOLCHAIN=local "$(GO_BIN)" version

bubblegun-build:
	@test -n "$(BUBBLEGUN_BUILD_DIR)" || { echo 'set a new BUBBLEGUN_BUILD_DIR'; exit 2; }
	python3 scripts/build-bubblegun.py --go "$(GO_BIN)" --output "$(BUBBLEGUN_BUILD_DIR)" --timeout "$(WATCHDOG)"

test:
	@test -n "$(RUN_DIR)" || { echo 'set a new RUN_DIR'; exit 2; }
	python3 scripts/safe-run.py --go "$(GO_BIN)" --package "$(PKG)" --mode test --test '$(TEST_PATTERN)' --run-dir "$(RUN_DIR)" --timeout "$(WATCHDOG)"

test-all:
	GO_BIN="$(GO_BIN)" sh scripts/run-tests.sh maintained

test-det:
	GOROOT="$(GO_ROOT)" GOTOOLCHAIN=local GODEBUG=asyncpreemptoff=1 "$(GO_BIN)" test -p=4 -count=1 -timeout=90s ./experiments/runtime/...

test-one:
	GOROOT="$(GO_ROOT)" GOTOOLCHAIN=local GODEBUG=asyncpreemptoff=1 "$(GO_BIN)" test -p=4 -count=1 -timeout=60s -run '^$(TEST)$$' ./$(PKG)/...

determinism:
	$(MAKE) test-one PKG=experiments/runtime TEST=TestDeterminism1000

test-ragate:
	GO_BIN="$(GO_BIN)" sh scripts/run-tests.sh ragate

test-resourcegc-smoke:
	@test -n "$(RUN_DIR)" || { echo 'set a new RUN_DIR'; exit 2; }
	python3 scripts/safe-run.py --go "$(GO_BIN)" --package experiments/resourcegc --mode test --test '^(TestLifecycleErrorPaths|TestCleanupSkipsOwnedResource|TestStaleCloseAfterCommitSucceeds|TestRunApplicationReturnsCompletedObservation|TestObservationOverflowIsReported)$$' --run-dir "$(RUN_DIR)" --timeout "$(WATCHDOG)"

test-raft:
	GO_BIN="$(GO_BIN)" sh scripts/run-tests.sh raft

test-etcd:
	GO_BIN="$(GO_BIN)" ETCD_PROFILE="$(ETCD_PROFILE)" sh scripts/run-tests.sh etcd

test-entrypoints:
	python3 -m unittest discover -s scripts -p 'test_*.py' -v
	$(MAKE) test-campaign-entrypoints

test-campaign-entrypoints:
	python3 -m unittest discover -s campaign -p 'runner_test.py' -v

clean:
	@echo 'No implicit cleanup. Remove only an explicitly reviewed disposable run directory yourself.'
