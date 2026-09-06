VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio/conduit-connector-s3.version=${VERSION}'" -o conduit-connector-s3 cmd/connector/main.go

.PHONY: test
test:
	go test $(GOTEST_FLAGS) -race ./...

# test-integration-s3 starts the MinIO container (test/docker-compose.yml),
# runs the S3-compatible-store integration tests (TestS3MinIO in destination,
# TestSource_MinIO in source) against it and stops the container again.
#
# Both packages are tested by a single `go test` invocation on purpose: an
# environment-variable prefix applies only to the command it precedes, so a
# `go test ./destination && go test ./source` shape would run the source tests
# with none of these variables set, they would skip, and the target would still
# exit 0. -p 1 keeps the two packages from running against MinIO concurrently.
#
# MINIO_ENDPOINT, not AWS_ENDPOINT_URL: the AWS SDK reads AWS_ENDPOINT_URL
# itself, so setting it would point the client at MinIO whether or not the
# connector's own aws.endpoint configuration works, and the tests would pass
# even with that plumbing removed. `env -u` additionally clears it out of the
# caller's environment; the tests fail loudly if it is set anyway.
.PHONY: test-integration-s3
test-integration-s3:
	docker compose -f test/docker-compose.yml up -d --wait
	@trap 'docker compose -f test/docker-compose.yml down' EXIT; \
		env -u AWS_ENDPOINT_URL \
		AWS_ACCESS_KEY_ID=conduitminio \
		AWS_SECRET_ACCESS_KEY=conduitminiosecret \
		AWS_S3_BUCKET=conduit-s3-minio-test \
		AWS_REGION=us-east-1 \
		MINIO_ENDPOINT=http://localhost:9000 \
		go test -race -count=1 -p 1 -run '^(TestS3MinIO|TestSource_MinIO)$$' ./destination ./source

.PHONY: lint
lint:
	golangci-lint run

.PHONY: fmt
fmt:
	gofumpt -l -w .

.PHONY: generate
generate:
	go generate ./...
	conn-sdk-cli readmegen -w

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools/go.mod
	@go list -modfile=tools/go.mod tool | xargs -I % go list -modfile=tools/go.mod -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy
