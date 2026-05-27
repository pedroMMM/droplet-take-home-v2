.PHONY: build run harness test clean

build:
	go build -o bin/server ./cmd/server
	go build -o bin/harness ./cmd/harness

run: build
	PORT=8080 ./bin/server

harness: build
	./bin/harness

test:
	go test ./...

clean:
	rm -rf bin/ webhook.db
