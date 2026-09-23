IMG ?= karpenter-provider-rafay:latest
LDFLAGS := "-X google.golang.org/protobuf/reflect/protoregistry.conflictPolicy=warn"

DEV_USER ?= ${USER}
DEV_TAG := registry.dev.rafay-edge.net/${DEV_USER}/karpenter-provider-rafay:$(shell git branch --show-current | tr "/" "-")-$(shell /bin/date "+%Y%m%d-%H%M")

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

.PHONY: build
build:
	DOCKER_BUILDKIT=1 docker build . -t ${IMG} --pull \
		--build-arg LDFLAGS=$(LDFLAGS) \
		--build-arg BUILD_USR=${BUILD_USER} \
		--build-arg BUILD_PWD=${BUILD_PASSWORD}

.PHONY: push
push: build tag-dev push-it

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
docker-build-arm64:
	DOCKER_BUILDKIT=1 docker build . -t ${IMG} --pull --platform linux/arm64 \
		--build-arg LDFLAGS=$(LDFLAGS) \
		--build-arg BUILD_USR=${BUILD_USER} \
		--build-arg BUILD_PWD=${BUILD_PASSWORD}

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
