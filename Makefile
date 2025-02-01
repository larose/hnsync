.PHONY: db.rm
db.rm:
	rm -f hn.db hn.db-shm hn.db-wal

.PHONY: format
format:
	go fmt

.PHONY: run
run:
	go run .

.PHONY: profile
profile:
	go tool pprof http://localhost:6060/debug/pprof/profile?seconds=10

.PHONY: build
build:
	go build -o hnsync

.PHONY: build-release
build-release:
	go build -ldflags="-s -w" -trimpath -o hnsync
