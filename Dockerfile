# Build stage: Rafay registry proxy for base image
# BuildKit sets TARGETOS/TARGETARCH from `docker build --platform ...` (use --platform to match cluster CPU).
FROM --platform=$BUILDPLATFORM registry-proxy.dev.rafay-edge.net/golang:1.24 AS builder

WORKDIR /src

# Copy dependency manifests first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source only (avoids "Copy file '.' excluded by .dockerignore" warning)
COPY cmd ./cmd
COPY pkg ./pkg
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags="-w -s" \
    -o /karpenter-provider-rafay \
    ./cmd/controller

# Runtime stage: minimal image (TLS to edge-broker uses system certs)
FROM gcr.io/distroless/static:latest
USER nonroot:nonroot
COPY --from=builder /karpenter-provider-rafay /karpenter-provider-rafay
ENTRYPOINT ["/karpenter-provider-rafay"]
