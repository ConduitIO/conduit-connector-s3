.PHONY: build test

build:
	go build -o conduit-connector-s3 cmd/s3/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

