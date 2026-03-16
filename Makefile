GO      ?= go
PKG     := ./...
MOD     := github.com/bbvtaev/pulse-core
BINARY  := pulse
PROTO   := api/proto/pulse.proto

BENCH_TIME ?= 10s

.PHONY: all
all: proto fmt test

.PHONY: proto
proto:
	protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO)

.PHONY: fmt
fmt:
	$(GO) fmt $(PKG)

.PHONY: tidy
tidy:
	$(GO) mod tidy

.PHONY: test
test:
	$(GO) test $(PKG)

.PHONY: test-race
test-race:
	$(GO) test -race $(PKG)

.PHONY: bench
bench:
	$(GO) test -run=^$$ -bench=. -benchmem -benchtime=$(BENCH_TIME) $(PKG)

.PHONY: bench-write
bench-write:
	$(GO) test -run=^$$ -bench=BenchmarkWriteThroughput -benchmem -benchtime=$(BENCH_TIME) $(PKG)

.PHONY: bench-prof
bench-prof:
	$(GO) test -run=^$$ -bench=BenchmarkWriteThroughput -benchmem \
		-benchtime=$(BENCH_TIME) \
		-cpuprofile=cpu.out -memprofile=mem.out $(PKG)
	@echo "CPU profile: cpu.out"
	@echo "Mem profile: mem.out"
	@echo "Run: go tool pprof cpu.out"

.PHONY: lint
lint:
	golangci-lint run ./...

.PHONY: build
build:
	$(GO) build -o bin/$(BINARY) ./cmd

.PHONY: build-all
build-all:
	$(GO) build $(PKG)

.PHONY: clean
clean:
	rm -f cpu.out mem.out
	rm -rf bin

.PHONY: version
version:
	@echo "Module:  $(MOD)"
	@echo "Go:      $$($(GO) version)"
