.PHONY: build test lint run local-up local-down smoke help

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

## local-up: Start backend + Grafana in Docker
local-up:
	docker compose -f deployments/docker/docker-compose.yml up -d --build

## local-down: Stop and remove Docker containers
local-down:
	docker compose -f deployments/docker/docker-compose.yml down

## smoke: Run API smoke test against a running backend (set BACKEND_ADDR to override localhost:8080)
smoke:
	bash tests/e2e/smoke.sh

## help: Show available make targets
help:
	@grep -E '^## [a-zA-Z_-]+:' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ": "}; {printf "\033[36m%-15s\033[0m %s\n", substr($$1,4), $$2}'
