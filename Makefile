.PHONY: build test

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio/conduit-connector-s3.version=${VERSION}'" -o conduit-connector-s3 cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

