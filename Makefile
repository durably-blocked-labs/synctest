# Synctest development Makefile

GO_ROOT := $(shell pwd)/go
GO_BIN := $(GO_ROOT)/bin/go
EXPERIMENTS := ./experiments

# Preemption flag (GOMAXPROCS set in code)
NO_PREEMPT := GODEBUG=asyncpreemptoff=1

.PHONY: build-go run test test-det clean help

help:
	@echo "Synctest Development"
	@echo ""
	@echo "Commands:"
	@echo "  make build-go       Build Go from source (go/)"
	@echo "  make test           Run all tests (preemption enabled)"
	@echo "  make test-det       Run all tests (preemption disabled)"
	@echo "  make go-version     Show custom Go version"
	@echo "  make clean          Clean build artifacts"
	@echo ""
	@echo "Manual Race Tests:"
	@echo "  make manual-race          Run manual_race_test.go (preemption enabled)"
	@echo "  make manual-race-det      Run manual_race_test.go (preemption disabled)"
	@echo ""
	@echo "Race Tests:"
	@echo "  make race                 Run race_test.go (preemption enabled)"
	@echo "  make race-det             Run race_test.go (preemption disabled)"
	@echo ""
	@echo "Specific Tests:"
	@echo "  make preempt-on           Run TestAsyncPreemptOff WITH preemption"
	@echo "  make preempt-off          Run TestAsyncPreemptOff WITHOUT preemption"
	@echo "  make determinism          Run TestDeterminism1000"
	@echo ""
	@echo "Note: GOMAXPROCS=1 is set in test code, asyncpreemptoff via env"

build-go:
	@echo "Building Go from source..."
	cd $(GO_ROOT)/src && ./make.bash
	@echo ""
	@echo "Done! Test with: make go-version"

go-version:
	$(GO_BIN) version

# === All Tests ===

test:
	$(GO_BIN) test -v $(EXPERIMENTS)/...

test-det:
	$(NO_PREEMPT) $(GO_BIN) test -v $(EXPERIMENTS)/...

# === manual_race_test.go ===

manual-race:
	$(GO_BIN) test -v $(EXPERIMENTS)/manual_race_test.go

manual-race-det:
	$(NO_PREEMPT) $(GO_BIN) test -v $(EXPERIMENTS)/manual_race_test.go

# === race_test.go ===

race:
	$(GO_BIN) test -v $(EXPERIMENTS)/race_test.go

race-det:
	$(NO_PREEMPT) $(GO_BIN) test -v $(EXPERIMENTS)/race_test.go

# === Specific Tests ===

preempt-on:
	$(GO_BIN) test -v -run TestAsyncPreemptOff $(EXPERIMENTS)/manual_race_test.go

preempt-off:
	$(NO_PREEMPT) $(GO_BIN) test -v -run TestAsyncPreemptOff $(EXPERIMENTS)/manual_race_test.go

determinism:
	$(NO_PREEMPT) $(GO_BIN) test -v -run TestDeterminism1000 $(EXPERIMENTS)/manual_race_test.go

# Run specific test by name
# Usage: make test-one TEST=TestManualRace
test-one:
	$(NO_PREEMPT) $(GO_BIN) test -v -run $(TEST) $(EXPERIMENTS)/...

clean:
	cd $(GO_ROOT)/src && ./clean.bash
