# Default local image tag (override: make build IMG=myrepo/karpenter-provider-rafay:v1)
IMG ?= karpenter-provider-rafay:latest

# Dev registry and tag: registry.dev.rafay-edge.net/<user>/karpenter-provider-rafay:<branch>-<date>-<time>
# If git has no branch name (detached HEAD, not a repo), use "main" so the tag is never ":-<date>".
DEV_USER ?= $(USER)
DEV_TAG := registry.dev.rafay-edge.net/$(DEV_USER)/karpenter-provider-rafay:$(shell b=$$(git branch --show-current 2>/dev/null | tr "/" "-"); if [ -z "$$b" ]; then b=main; fi; echo $$b)-$(shell /bin/date "+%Y%m%d-%H%M")

# Build the controller binary
.PHONY: build
build:
	go build -o bin/karpenter-provider-rafay ./cmd/controller

# Download dependencies
.PHONY: deps
deps:
	go mod tidy
	go mod download

# Run the controller locally (uses KUBECONFIG). Set EDGE_CLIENT_CERT_FOLDER or CERT_FOLDER (edge-broker TLS).
.PHONY: run
run: build
	./bin/karpenter-provider-rafay

.PHONY: run-fast
run-fast:
	go run ./cmd/controller

# Install CRDs then controller RBAC/Deployment (explicit files so a stray kustomization.yaml in config/deploy/ is ignored).
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

# --- Docker (aligned with edgesrv) ---
# "exec format error" at runtime means the image CPU arch does not match the node (e.g. arm64 image on amd64).
# Use docker-build-amd64 for typical x86 clusters; docker-build-arm64 for ARM nodes (e.g. Graviton).
# Default docker-build targets linux/amd64 so builds on Apple Silicon still run on common clusters.

.PHONY: docker-build
docker-build: docker-build-amd64

.PHONY: docker-build-amd64
docker-build-amd64:
	DOCKER_BUILDKIT=1 docker build . -t $(IMG) --pull --platform linux/amd64

.PHONY: docker-build-arm64
docker-build-arm64:
	DOCKER_BUILDKIT=1 docker build . -t $(IMG) --pull --platform linux/arm64

# Tag local IMG as DEV_TAG and push to registry.dev.rafay-edge.net
.PHONY: push-it
push-it:
	docker tag $(IMG) $(DEV_TAG)
	docker push $(DEV_TAG)

# Build then push to dev registry
.PHONY: push
push: docker-build push-it

# Install CRDs into current cluster
.PHONY: install-crds
install-crds:
	kubectl apply -f config/crd/

# Generate CRDs (controller-gen)
.PHONY: generate
generate:
	controller-gen object:headerFile=hack/boilerplate.go.txt paths=./pkg/apis/...
	controller-gen crd paths=./pkg/apis/... output:crd:dir=config/crd

# Clean build artifacts
.PHONY: clean
clean:
	rm -rf bin/
