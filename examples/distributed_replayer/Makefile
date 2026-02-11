GO_ROOT := $(shell pwd)/../../go
GO_BIN := $(GO_ROOT)/bin/go
NO_PREEMPT := GODEBUG=asyncpreemptoff=1

.PHONY: test

test:
	$(NO_PREEMPT) GOROOT=$(GO_ROOT) $(GO_BIN) test -v -count=1 -timeout 10s .
