#!/usr/bin/env bash
# Runs the ambient-credentials integration tests (ambient_minio_test.go)
# against a TLS MinIO with an ephemeral host port. No AWS account is used:
# credentials come from MinIO's own STS through a mocked IMDS and a mocked STS
# web-identity endpoint inside the test.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
project="${COMPOSE_PROJECT_NAME:-s3-ambient-$$}"
compose=(docker compose -p "$project" -f "$root/test/docker-compose-ambient.yml")

certs="$(mktemp -d)"
export AMBIENT_CERTS_DIR="$certs"
cleanup() {
  "${compose[@]}" down -v >/dev/null 2>&1 || true
  rm -rf "$certs"
}
trap cleanup EXIT

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$certs/private.key" -out "$certs/public.crt" \
  -subj "/CN=localhost" -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" 2>/dev/null
chmod 0644 "$certs/private.key" # read by the container user; throwaway key

"${compose[@]}" up -d --wait
hostport="$("${compose[@]}" port minio 9000)"
port="${hostport##*:}"

cd "$root"
env -u AWS_ENDPOINT_URL -u AWS_ENDPOINT_URL_S3 -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN \
  -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy \
  MINIO_TLS_ENDPOINT="https://127.0.0.1:$port" \
  MINIO_TLS_CA="$certs/public.crt" \
  MINIO_ROOT_USER=conduitminio \
  MINIO_ROOT_PASSWORD=conduitminiosecret \
  go test -race -count=1 -v -run '^TestMinIOAmbient_' .
