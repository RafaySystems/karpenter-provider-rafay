# syntax=docker/dockerfile:1.6
# Self-contained build: the private github.com/RafaySystems/* modules (edge-common and the Karpenter fork
# github.com/RafaySystems/karpenter-rafay that the go.mod replace points at) are fetched with BUILD_USR/BUILD_PWD
# (GitHub credentials), so no extra build context is needed:
#   docker build --build-arg BUILD_USR=... --build-arg BUILD_PWD=... .
FROM --platform=$BUILDPLATFORM registry-proxy.dev.rafay-edge.net/golang:1.26-alpine AS builder

RUN apk add --no-cache git build-base

WORKDIR /workspace

ARG BUILD_USR
ARG BUILD_PWD

COPY . ./

RUN test -n "${BUILD_USR}" && \
    test -n "${BUILD_PWD}" && \
    echo machine github.com login ${BUILD_USR} password ${BUILD_PWD} > ~/.netrc && \
    chmod 400 ~/.netrc && \
    GOPRIVATE='github.com/RafaySystems/*' go mod download

ARG TARGETARCH TARGETOS
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags "-X google.golang.org/protobuf/reflect/protoregistry.conflictPolicy=warn" -a -o karpenter-provider-rafay.big ./cmd/controller

RUN [ "$TARGETARCH" = "amd64" ] && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test -ldflags "-X google.golang.org/protobuf/reflect/protoregistry.conflictPolicy=warn" -count=1 -short ./pkg/... ./cmd/... || echo 'arm64 platform'

FROM --platform=$BUILDPLATFORM registry-proxy.dev.rafay-edge.net/rafaysystems/upx:latest AS upx
WORKDIR /workspace
COPY --from=builder /workspace/karpenter-provider-rafay.big .
ARG TARGETARCH
RUN [ "${TARGETARCH}" = "amd64" ] && upx --best --lzma -o /workspace/karpenter-provider-rafay.upx /workspace/karpenter-provider-rafay.big || cp /workspace/karpenter-provider-rafay.big /workspace/karpenter-provider-rafay.upx

FROM gcr.io/distroless/static:latest
USER nonroot
ENTRYPOINT ["/karpenter-provider-rafay"]
COPY --from=upx /workspace/karpenter-provider-rafay.upx /karpenter-provider-rafay
