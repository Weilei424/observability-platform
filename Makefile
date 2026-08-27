.PHONY: build test lint run local-up local-down local-logs local-reset smoke smoke-logs smoke-compose smoke-kind bench bench-go bench-k6 help

## build: Compile the backend binary
build:
	go build ./...

## test: Run all unit tests
test:
	go test ./...

## lint: Run golangci-lint (must be installed separately)
lint:
	@which golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not found. Install from https://golangci-lint.run/usage/install/"; \
		exit 1; \
	}
	golangci-lint run ./...

## run: Start the backend locally with go run
run:
	go run ./cmd/server

## local-up: Start the demo stack in Docker (backend, Grafana, load generator, sample app)
local-up:
	docker compose -f deployments/docker/docker-compose.yml up -d --build

## local-down: Stop and remove Docker containers
local-down:
	docker compose -f deployments/docker/docker-compose.yml down

## local-logs: Follow logs from the running demo stack
local-logs:
	docker compose -f deployments/docker/docker-compose.yml logs -f

# local-down deliberately keeps the volumes: data surviving a stack restart is
# the durability story the demo tells, so discarding it has to be explicit.
## local-reset: Stop the demo and delete its data and Grafana volumes
local-reset:
	docker compose -f deployments/docker/docker-compose.yml down -v

## smoke: Run metrics + logs API smoke tests against a running backend (set BACKEND_ADDR to override localhost:8080)
smoke:
	bash tests/e2e/smoke.sh
	$(MAKE) --no-print-directory smoke-logs

## smoke-logs: Validate Grafana provisioning, then run the Loki API smoke test
# The Go step covers the provisioning files (no backend needed). It runs the
# whole package rather than a -run filter, because a filter that matches nothing
# exits 0 and would silently skip every provisioning check.
smoke-logs:
	go test ./tests/e2e/ -count=1
	bash tests/e2e/logs_smoke.sh

## smoke-compose: Bring up the Compose stack and test it through Grafana's API (needs Docker; ports 3000/8080 free)
smoke-compose:
	bash tests/e2e/compose_smoke.sh

## smoke-kind: Deploy all three Helm charts into a kind cluster and test the restart/persistence path (needs kind, kubectl, helm, docker)
smoke-kind:
	bash tests/e2e/kind_smoke.sh

## bench-go: Run Go micro-benchmarks (storage/query engine, in-process)
bench-go:
	go test -bench=. -benchmem -run='^$$' ./internal/...

## bench-k6: Run end-to-end k6 HTTP load tests (hermetic; builds + starts backend)
bench-k6:
	bash bench/run.sh

## bench: Run Go benchmarks and k6 load tests
bench: bench-go bench-k6

## help: Show available make targets
help:
	@grep -E '^## [a-zA-Z0-9_-]+:' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ": "}; {printf "\033[36m%-15s\033[0m %s\n", substr($$1,4), $$2}'
