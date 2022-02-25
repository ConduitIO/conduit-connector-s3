.PHONY: build test

build:
	go build -o conduit-plugin-s3 cmd/s3/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

