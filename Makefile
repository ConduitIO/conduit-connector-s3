.PHONY: build test

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-s3.version=${VERSION}'" -o conduit-connector-s3 cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

