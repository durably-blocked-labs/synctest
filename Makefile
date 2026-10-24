# Explicit maintained/legacy module boundaries; no target deletes experiment data.
GO_ROOT ?= $(CURDIR)/go
GO_BIN ?= $(GO_ROOT)/bin/go
TEST ?= TestThreeVoterMemoryServiceSingleBubble
PKG ?= experiments/raft
K ?= 2
KMAX ?= 3
ATTEMPTS ?= 1
WATCHDOG ?= 60
ETCD_PROFILE ?=
# No default destination: each campaign must explicitly own a NEW directory.
RUN_DIR ?=
OUTDIR ?= charts/data
BUBBLEGUN_BUILD_DIR ?=

.PHONY: help build-go go-version test test-all test-det test-one determinism \
		test-raft test-etcd test-legacy-regressions test-legacy-raft test-entrypoints verify-archive \
		benchmark-charts charts-collect charts-sweep charts-plot bubblegun-build clean
help:
	@echo 'test-all: maintained root packages, not historical bug searches or nested etcd'
	@echo 'test-etcd: explicit full-server loopback/files baseline (not controlled simulation)'
	@echo 'test-legacy-regressions: retained quorum characterization in legacy module'
	@echo 'test-legacy-raft TEST_PATTERN=pattern: historical fixture, known pinned-Raft compile blocker'
	@echo 'benchmark-charts pkg=bugs/ra-gate RUN_DIR=/new/exclusive/run (collect only)'
	@echo 'charts-plot OUTDIR=/run/data FIGURE_DIR=/new/exclusive/figures (explicit)'
	@echo 'bubblegun-build BUBBLEGUN_BUILD_DIR=/new/directory: build Bubble Gun with saved watchdog evidence'

build-go:
	bash scripts/build-go.sh

go-version:
	GOROOT="$(GO_ROOT)" GOTOOLCHAIN=local "$(GO_BIN)" version

bubblegun-build:
	@test -n "$(BUBBLEGUN_BUILD_DIR)" || { echo 'set a new BUBBLEGUN_BUILD_DIR'; exit 2; }
	python3 scripts/build-bubblegun.py --go "$(GO_BIN)" --output "$(BUBBLEGUN_BUILD_DIR)" --timeout "$(WATCHDOG)"

test:
	@test -n "$(pkg)" || { echo 'set pkg=package/path'; exit 2; }
	python3 scripts/safe-run.py --go "$(GO_BIN)" --package "$(pkg)" --mode test --test '$(or $(TEST_PATTERN),.)' --run-dir "$(RUN_DIR)" --timeout "$(WATCHDOG)"

test-all:
	GO_BIN="$(GO_BIN)" sh scripts/run-tests.sh maintained

test-det:
	GOROOT="$(GO_ROOT)" GOTOOLCHAIN=local GODEBUG=asyncpreemptoff=1 "$(GO_BIN)" test -count=1 -timeout=90s ./experiments/runtime/...

test-one:
	GOROOT="$(GO_ROOT)" GOTOOLCHAIN=local GODEBUG=asyncpreemptoff=1 "$(GO_BIN)" test -count=1 -timeout=60s -run '^$(TEST)$$' ./$(PKG)/...

determinism:
	$(MAKE) test-one PKG=experiments/runtime TEST=TestDeterminism1000

test-raft:
	GO_BIN="$(GO_BIN)" sh scripts/run-tests.sh raft

test-etcd:
	GO_BIN="$(GO_BIN)" ETCD_PROFILE="$(ETCD_PROFILE)" sh scripts/run-tests.sh etcd

test-legacy-regressions:
	GO_BIN="$(GO_BIN)" sh scripts/run-tests.sh legacy-regressions

test-legacy-raft:
	@test -n "$(TEST_PATTERN)" || { echo 'set explicit TEST_PATTERN (historical Raft compilation is currently blocked)'; exit 2; }
	GO_BIN="$(GO_BIN)" sh scripts/run-tests.sh legacy-raft '$(TEST_PATTERN)'

test-entrypoints:
	python3 -m unittest discover -s scripts -p 'test_*.py' -v

verify-archive:
	python3 scripts/verify-archive.py

benchmark-charts:
	@test -n "$(pkg)" -a -n "$(RUN_DIR)" || { echo 'set pkg and a new RUN_DIR'; exit 2; }
	python3 scripts/safe-run.py --go "$(GO_BIN)" --package "$(pkg)" --run-dir "$(RUN_DIR)" --attempts "$(ATTEMPTS)" --timeout "$(WATCHDOG)"

charts-collect:
	@test -n "$(RUN_DIR)" || { echo 'set a new RUN_DIR'; exit 2; }
	EXPLORE_K=$(K) python3 scripts/safe-run.py --go "$(GO_BIN)" --package "$(PKG)" --mode test --test '^$(TEST)$$' --run-dir "$(RUN_DIR)" --timeout "$(WATCHDOG)"

charts-sweep:
	@test -n "$(RUN_DIR)" || { echo 'set a new RUN_DIR'; exit 2; }
	@mkdir "$(RUN_DIR)" || exit 2; \
	for k in $$(seq 0 $(KMAX)); do \
		$(MAKE) charts-collect K=$$k RUN_DIR="$(RUN_DIR)/k-$$k" || exit $$?; \
	done

charts-plot:
	@test -n "$(FIGURE_DIR)" || { echo 'set a new FIGURE_DIR'; exit 2; }
	@mkdir "$(FIGURE_DIR)" || exit 2
	python3 charts/charts.py --data "$(OUTDIR)" --out "$(FIGURE_DIR)"

clean:
	@echo 'No implicit cleanup. Remove only an explicitly reviewed disposable run directory yourself.'
