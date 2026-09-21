test:
	go test ./... -count=1

benchmark:
	go run ./cmd/benchmark

run-server:
	CLOCK_MODE=manual CLOCK_START=2026-09-20T00:00:00Z PORT=8080 BASE_DELAY_MS=200 go run ./cmd/server

build:
	go build -o bin/server ./cmd/server
	go build -o bin/benchmark ./cmd/benchmark
