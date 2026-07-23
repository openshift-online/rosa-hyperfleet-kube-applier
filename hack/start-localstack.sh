#!/usr/bin/env bash
# start-localstack.sh — starts a LocalStack container with DynamoDB,
# DynamoDB Streams, SQS, and SNS enabled in detached mode, then waits for it
# to be healthy.
#
# Idempotent: if the container is already running it is reused.
# A stopped/dead container is replaced.
#
# Environment variables:
#   CONTAINER_ENGINE      podman or docker (auto-detected if unset)
#   LOCALSTACK_PORT       host port (default: 4566)
#   LOCALSTACK_AUTH_TOKEN set to use the Pro image

set -euo pipefail

CONTAINER_ENGINE="${CONTAINER_ENGINE:-$(command -v podman 2>/dev/null || command -v docker 2>/dev/null)}"
CONTAINER_NAME="localstack-kube-applier-aws"
PORT="${LOCALSTACK_PORT:-4566}"
HEALTH_URL="http://127.0.0.1:${PORT}/_localstack/health"
HEALTH_TIMEOUT=60

if [[ -n "${LOCALSTACK_AUTH_TOKEN:-}" ]]; then
  IMAGE="localstack/localstack-pro"
  AUTH_ARGS=(-e "LOCALSTACK_AUTH_TOKEN=${LOCALSTACK_AUTH_TOKEN}")
else
  IMAGE="localstack/localstack"
  AUTH_ARGS=()
fi

wait_healthy() {
  echo "Waiting for LocalStack at ${HEALTH_URL} ..."
  local i=0
  until curl -sf "${HEALTH_URL}" >/dev/null 2>&1; do
    i=$((i + 1))
    if [[ ${i} -ge ${HEALTH_TIMEOUT} ]]; then
      echo "ERROR: LocalStack did not become healthy within ${HEALTH_TIMEOUT}s." >&2
      exit 1
    fi
    sleep 1
  done
  echo "LocalStack is healthy."
}

# Reuse if already running.
if "${CONTAINER_ENGINE}" inspect "${CONTAINER_NAME}" --format '{{.State.Status}}' 2>/dev/null | grep -q "^running$"; then
  echo "LocalStack container '${CONTAINER_NAME}' already running on port ${PORT}."
  wait_healthy
  exit 0
fi

# Remove any stale container.
"${CONTAINER_ENGINE}" rm -f "${CONTAINER_NAME}" 2>/dev/null || true

echo "Starting ${IMAGE} on 127.0.0.1:${PORT} ..."
"${CONTAINER_ENGINE}" run -d \
  --name "${CONTAINER_NAME}" \
  -p "127.0.0.1:${PORT}:4566" \
  -e "SERVICES=dynamodb,dynamodbstreams,sqs,sns" \
  -e "DEBUG=0" \
  "${AUTH_ARGS[@]}" \
  "${IMAGE}"

wait_healthy
echo "LocalStack ready on http://127.0.0.1:${PORT}."
echo "Stop with: ${CONTAINER_ENGINE} stop ${CONTAINER_NAME}"
