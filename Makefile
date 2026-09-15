IMG ?= karpenter-provider-rafay:latest
LDFLAGS := "-X google.golang.org/protobuf/reflect/protoregistry.conflictPolicy=warn"

# go.mod replace: ../edge-common — pass --build-context edgecommon=$(EDGE_COMMON_DIR)
EDGE_COMMON_DIR ?= ../edge-common

DEV_USER ?= ${USER}
DEV_TAG := registry.dev.rafay-edge.net/${DEV_USER}/karpenter-provider-rafay:$(shell git branch --show-current | tr "/" "-")-$(shell /bin/date "+%Y%m%d-%H%M")

# Shared by every image target (Dockerfile needs GitHub creds for private modules + the edge-common context).
DOCKER_BUILD_ARGS := --build-arg LDFLAGS=$(LDFLAGS) \
	--build-arg BUILD_USR=${BUILD_USER} \
	--build-arg BUILD_PWD=${BUILD_PASSWORD} \
	--build-context edgecommon=$(EDGE_COMMON_DIR)

# Multi-arch image (docker buildx). The Dockerfile cross-compiles from $BUILDPLATFORM to
# $TARGETARCH, so one buildx invocation yields a single manifest list for every platform below.
# A manifest list cannot be loaded into the local image store, so push-multiarch pushes straight
# to $(MULTIARCH_IMG) instead of going through tag-dev/push-it.
PLATFORMS ?= linux/amd64,linux/arm64
BUILDX_BUILDER ?= multi-arch-builder
MULTIARCH_IMG ?= $(DEV_TAG)

.PHONY: update-deps
update-deps:
	GOPRIVATE=github.com/RafaySystems/* go get -d github.com/RafaySystems/rafay-common@master
	GOPRIVATE=github.com/RafaySystems/* go get -d github.com/RafaySystems/edge-common@main
	$(MAKE) tidy

.PHONY: push-it
push-it:
	docker push $(DEV_TAG)

.PHONY: tag-dev
tag-dev:
	docker tag ${IMG} $(DEV_TAG)

.PHONY: check-edge-common
check-edge-common:
	@test -d "$(EDGE_COMMON_DIR)" || (echo "edge-common not found at $(EDGE_COMMON_DIR); set EDGE_COMMON_DIR" >&2; exit 1)

# Single-arch image for the host platform, loaded into the local image store as $(IMG).
.PHONY: build
build: check-edge-common
	DOCKER_BUILDKIT=1 docker build . -t ${IMG} --pull $(DOCKER_BUILD_ARGS)

.PHONY: push
push: build tag-dev push-it

# Create the docker-container builder used for multi-arch builds if it does not exist yet.
# Does not change the default builder; the multi-arch targets select it with --builder.
.PHONY: buildx-setup
buildx-setup:
	@docker buildx inspect $(BUILDX_BUILDER) >/dev/null 2>&1 || \
		docker buildx create --name $(BUILDX_BUILDER) --driver docker-container --bootstrap

# Cross-compile for every platform in $(PLATFORMS) without producing an image (build cache only).
.PHONY: build-multiarch
build-multiarch: check-edge-common buildx-setup
	docker buildx build . --builder $(BUILDX_BUILDER) --platform $(PLATFORMS) --pull \
		--provenance=false \
		--output type=cacheonly \
		$(DOCKER_BUILD_ARGS)

# Build $(PLATFORMS) and push one multi-arch manifest list as $(MULTIARCH_IMG) (default: $(DEV_TAG)).
.PHONY: push-multiarch
push-multiarch: check-edge-common buildx-setup
	docker buildx build . --builder $(BUILDX_BUILDER) --platform $(PLATFORMS) --pull --push \
		--provenance=false \
		-t $(MULTIARCH_IMG) \
		$(DOCKER_BUILD_ARGS)

# Local compile (linux/arm64) + distroless:debug image pushed as $(DEV_TAG) — same pattern as edgesrv build-dev / push-dev.
.PHONY: build-dev
build-dev: check
	rm -f karpenter-provider-rafay.big
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOPRIVATE=github.com/RafaySystems/* go build -ldflags $(LDFLAGS) -a -o karpenter-provider-rafay.big ./cmd/controller
	docker build --platform linux/amd64 --push -f Dockerfile.dev -t ${DEV_TAG} .

.PHONY: push-dev
push-dev: build-dev push-it

.PHONY: compile
compile:
	go build -ldflags $(LDFLAGS) -o bin/karpenter-provider-rafay ./cmd/controller

.PHONY: check
check: tidy
	go fmt ./...

.PHONY: tidy
tidy:
	GOPRIVATE=github.com/RafaySystems/* go mod tidy

.PHONY: vendor
vendor:
	GOPRIVATE=github.com/RafaySystems/* go mod vendor

.PHONY: deps
deps: tidy
	go mod download

.PHONY: run
run: compile
	./bin/karpenter-provider-rafay

.PHONY: run-fast
run-fast:
	go run ./cmd/controller

.PHONY: deploy-crd
deploy-crd:
	kubectl apply -f config/crd/

.PHONY: deploy-apply
deploy-apply:
	kubectl apply -f config/deploy/namespace-karpenter-leader.yaml
	kubectl apply -f config/deploy/serviceaccount.yaml
	kubectl apply -f config/deploy/rbac.yaml
	kubectl apply -f config/deploy/deployment.yaml

.PHONY: deploy
deploy: deploy-crd deploy-apply

.PHONY: docker-build
docker-build: build

.PHONY: docker-build-amd64
docker-build-amd64: build

.PHONY: docker-build-arm64
docker-build-arm64: check-edge-common
	DOCKER_BUILDKIT=1 docker build . -t ${IMG} --pull --platform linux/arm64 $(DOCKER_BUILD_ARGS)

.PHONY: docker-build-multiarch
docker-build-multiarch: build-multiarch

.PHONY: install-crds
install-crds:
	kubectl apply -f config/crd/

.PHONY: generate
generate:
	controller-gen object:headerFile=hack/boilerplate.go.txt paths=./pkg/apis/...
	controller-gen crd paths=./pkg/apis/... output:crd:dir=config/crd

.PHONY: clean
clean:
	rm -rf bin/
	rm -f karpenter-provider-rafay.big
