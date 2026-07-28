.PHONY: help build desire-tool desirectl \
	test test-unit test-integration \
	fmt vet lint verify \
	image image-push \
	infra-up infra-down localstack kind-setup \
	run-local clean

# ── Configuration ────────────────────────────────────────────────────────

BINARY   ?= kube-applier-aws
IMAGE_REPO ?= kube-applier-aws
IMAGE_TAG  ?= latest
GIT_SHA    := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GOOS       ?= linux
GOARCH     ?= amd64

LOCALSTACK_PORT    ?= 4566
KIND_CLUSTER_NAME  ?= kube-applier-dev

CONTAINER_ENGINE ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)

TOOLS_DIR     := ./hack/tools
TOOLS_BIN_DIR := $(TOOLS_DIR)/bin
GOLANGCI_LINT := $(abspath $(TOOLS_BIN_DIR)/golangci-lint)

$(GOLANGCI_LINT): $(TOOLS_DIR)/go.mod
	cd $(TOOLS_DIR); go build -tags=tools -o $(abspath $(TOOLS_BIN_DIR))/golangci-lint github.com/golangci/golangci-lint/v2/cmd/golangci-lint

# ── Help ─────────────────────────────────────────────────────────────────

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Build:"
	@echo "  build            Build the kube-applier-aws binary"
	@echo "  desire-tool      Build the desire-tool binary"
	@echo "  desirectl        Build the desirectl binary"
	@echo ""
	@echo "Test:"
	@echo "  test             All tests (unit + integration)"
	@echo "  test-unit        Unit tests (no external services)"
	@echo "  test-integration Integration tests (starts LocalStack + Kind if needed)"
	@echo "                   Override kubeconfig with KIND_KUBECONFIG=/path/to/config"
	@echo ""
	@echo "Code Quality:"
	@echo "  fmt              go fmt on all packages"
	@echo "  vet              go vet on all packages"
	@echo "  lint             golangci-lint on all packages"
	@echo "  verify           go mod tidy + check for drift"
	@echo ""
	@echo "Images:"
	@echo "  image            Build container image (podman or docker)"
	@echo "  image-push       Build and push container image"
	@echo ""
	@echo "Infrastructure:"
	@echo "  infra-up         Ensure LocalStack and Kind cluster are running (idempotent)"
	@echo "  infra-down       Tear down LocalStack and Kind cluster"
	@echo "  localstack       Start LocalStack container (idempotent)"
	@echo "  kind-setup       Create Kind cluster and apply RBAC (idempotent)"
	@echo "  run-local        Run the controller locally against Kind + LocalStack"
	@echo ""
	@echo "  clean            Remove built binaries"

# ── Build ────────────────────────────────────────────────────────────────

build:
	go build -o $(BINARY) .

desire-tool:
	go build -o desire-tool ./cmd/desire-tool

desirectl:
	go build -o desirectl ./cmd/desirectl

KIND_KUBECONFIG ?= /tmp/$(KIND_CLUSTER_NAME).kubeconfig

# ── Test ─────────────────────────────────────────────────────────────────

test: test-unit test-integration

test-unit:
	GOTELEMETRY=off go test -vet=off -race -count=1 ./...

# test-integration runs controller-level tests that need both LocalStack and a
# Kind cluster. infra-up is run first and is idempotent — existing containers
# and clusters are reused. Use 'make infra-down' to tear everything down.
test-integration: infra-up
	GOTELEMETRY=off \
	LOCALSTACK_ENDPOINT=http://127.0.0.1:$(LOCALSTACK_PORT) \
	KUBECONFIG=$(KIND_KUBECONFIG) \
	go test -vet=off -race -v -count=1 -timeout 120s ./test/integration/...

# ── Code Quality ─────────────────────────────────────────────────────────

fmt:
	go fmt ./...

vet:
	go vet ./...

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run --config .golangci.yml --timeout 5m ./...

verify:
	go mod tidy
	git diff --exit-code go.mod go.sum

# ── Images ───────────────────────────────────────────────────────────────

image:
	$(CONTAINER_ENGINE) build -f Containerfile \
		--platform $(GOOS)/$(GOARCH) \
		-t $(IMAGE_REPO):$(IMAGE_TAG) .
	$(CONTAINER_ENGINE) tag $(IMAGE_REPO):$(IMAGE_TAG) $(IMAGE_REPO):$(GIT_SHA)

image-push: image
	$(CONTAINER_ENGINE) push $(IMAGE_REPO):$(IMAGE_TAG)
	$(CONTAINER_ENGINE) push $(IMAGE_REPO):$(GIT_SHA)

# ── Infrastructure ────────────────────────────────────────────────────────

# infra-up ensures both LocalStack and Kind are running. Both steps are
# idempotent: already-running containers and existing clusters are reused.
infra-up: localstack kind-setup

infra-down:
	$(CONTAINER_ENGINE) rm -f localstack-kube-applier-aws 2>/dev/null || true
	kind delete cluster --name $(KIND_CLUSTER_NAME) 2>/dev/null || true

localstack:
	CONTAINER_ENGINE=$(CONTAINER_ENGINE) LOCALSTACK_PORT=$(LOCALSTACK_PORT) \
	./hack/start-localstack.sh

kind-setup:
	KIND_CLUSTER_NAME=$(KIND_CLUSTER_NAME) \
	KIND_KUBECONFIG=$(KIND_KUBECONFIG) \
	./hack/setup-kind.sh

run-local:
	./hack/run-local.sh

# ── Clean ────────────────────────────────────────────────────────────────

clean:
	rm -f $(BINARY) desire-tool desirectl
