# Synctest development Makefile

GO_ROOT := $(CURDIR)/go
GO_BIN  := $(GO_ROOT)/bin/go
export GOROOT := $(GO_ROOT)

.PHONY: build-go test test-all test-det go-version clean help \
        charts-collect charts-sweep charts-plot benchmark-charts

# Charts defaults (override on command line)
TEST    ?= TestRaftThreeNodeElectionExplore
PKG     ?= ./rafttest/...
K       ?= 2
KMAX    ?= 3
OUTDIR  ?= charts/data

help:
	@echo "Synctest Development"
	@echo ""
	@echo "  make test pkg=bugs/ra-gate               Run one package"
	@echo "  make test-all                           Run all packages"
	@echo "  make benchmark-charts pkg=bugs/ra-gate  Run TestBench_ and render package-local charts"
	@echo "  make build-go                           Build Go from source"
	@echo "  make go-version                         Show custom Go version"
	@echo "  make clean                              Clean build artifacts"
	@echo ""
	@echo "Charts (requires NewJSONLObserver wired into test — see orchestrator/metrics.go)"
	@echo ""
	@echo "  make charts-collect TEST=TestFoo K=2    Single systematic run at k=K"
	@echo "  make charts-collect TEST=TestFoo PKG=./some-package/... K=2"
	@echo "  make charts-sweep   TEST=TestFoo KMAX=3 Sweep k=0..KMAX"
	@echo "  make charts-plot                        Generate all figures from collected data"

build-go:
	cd $(GO_ROOT)/src && ./make.bash

go-version:
	$(GO_BIN) version

# === Tests ===
# make test pkg=bugs/ra-gate
# make test-all

ifdef pkg
test:
	$(GO_BIN) test -v -count=1 ./$(pkg)/...
else
test:
	@echo "usage: make test pkg=bugs/ra-gate"
	@echo "       make test-all"
endif

test-all:
	$(GO_BIN) test -v -count=1 ./explorer/... ./experiments/... ./orchestrator/... ./rafttest/... ./bugs/...

test-det:
	GODEBUG=asyncpreemptoff=1 $(GO_BIN) test -v -count=1 ./experiments/...

clean:
	cd $(GO_ROOT)/src && ./clean.bash

# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

# Single systematic run at context bound K.
# Requires the test to call orchestrator.ObserverFromEnv(t), BoundFromEnv(),
# and MaxRunsFromEnv() — see orchestrator/metrics.go for the one-liner.
charts-collect:
	@mkdir -p $(OUTDIR)
	GODEBUG=asyncpreemptoff=1 \
	EXPLORE_K=$(K) \
	METRICS_FILE=$(CURDIR)/$(OUTDIR)/$(TEST)_k$(K)_sys.jsonl \
	$(GO_BIN) test -v -count=1 -run $(TEST) $(PKG)

# Sweep k=0..KMAX, one JSONL file per k.
charts-sweep:
	@for k in $(shell seq 0 $(KMAX)); do \
		echo "--- sweep k=$$k ---"; \
		$(MAKE) charts-collect TEST=$(TEST) PKG=$(PKG) K=$$k OUTDIR=$(OUTDIR); \
	done

# Generate all figures from collected data in charts/data/.
charts-plot:
	@python3 -c "import matplotlib,pandas,numpy,scipy" 2>/dev/null || \
		pip3 install -q -r charts/requirements.txt
	python3 charts/charts.py \
		--input "$(OUTDIR)/*.jsonl" \
		--outdir charts/figures

ifdef pkg
benchmark-charts:
	@mkdir -p "$(CURDIR)/$(patsubst ./%,%,$(pkg))/benchmarking/data" "$(CURDIR)/$(patsubst ./%,%,$(pkg))/benchmarking/figures"
	@rm -f "$(CURDIR)/$(patsubst ./%,%,$(pkg))/benchmarking/data"/*.jsonl "$(CURDIR)/$(patsubst ./%,%,$(pkg))/benchmarking/data"/*.json "$(CURDIR)/$(patsubst ./%,%,$(pkg))/benchmarking/figures"/*
	@python3 -c "import matplotlib,numpy" 2>/dev/null || \
		pip3 install -q -r charts/requirements.txt
	MPLBACKEND=Agg \
	MPLCONFIGDIR=/tmp/matplotlib \
	BENCH_DIR="$(CURDIR)/$(patsubst ./%,%,$(pkg))/benchmarking/data" \
	GODEBUG=asyncpreemptoff=1 \
	$(GO_BIN) test -v -count=1 -run 'TestBench_' ./$(patsubst ./%,%,$(pkg))
	MPLBACKEND=Agg \
	MPLCONFIGDIR=/tmp/matplotlib \
	python3 charts/charts.py \
		--data "$(CURDIR)/$(patsubst ./%,%,$(pkg))/benchmarking/data" \
		--out "$(CURDIR)/$(patsubst ./%,%,$(pkg))/benchmarking/figures"
else
benchmark-charts:
	@echo "usage: make benchmark-charts pkg=bugs/ra-gate"
endif
