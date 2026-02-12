# Synctest development Makefile

GO_ROOT := $(CURDIR)/go
GO_BIN  := $(GO_ROOT)/bin/go
export GOROOT := $(GO_ROOT)

.PHONY: build-go test test-all test-det go-version clean help

help:
	@echo "Synctest Development"
	@echo ""
	@echo "  make test pkg=examples/local_replayer   Run one package"
	@echo "  make test-all                           Run all packages"
	@echo "  make build-go                           Build Go from source"
	@echo "  make go-version                         Show custom Go version"
	@echo "  make clean                              Clean build artifacts"

build-go:
	cd $(GO_ROOT)/src && ./make.bash

go-version:
	$(GO_BIN) version

# === Tests ===
# make test pkg=examples/local_replayer
# make test-all

ifdef pkg
test:
	$(GO_BIN) test -v -count=1 ./$(pkg)/...
else
test:
	@echo "usage: make test pkg=examples/local_replayer"
	@echo "       make test-all"
endif

test-all:
	$(GO_BIN) test -v -count=1 ./examples/... ./explorer/... ./experiments/...

test-det:
	GODEBUG=asyncpreemptoff=1 $(GO_BIN) test -v -count=1 ./experiments/...

clean:
	cd $(GO_ROOT)/src && ./clean.bash
