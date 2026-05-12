# Multi-stage Dockerfile for building Go binaries for multiple architectures

ARG GO_VERSION=1.21

FROM --platform=${BUILDPLATFORM} docker.io/golang:${GO_VERSION} AS builder

WORKDIR /opt/synapse-compressor/

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build for the target platform
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o synapse_compress_state ./cmd/synapse_compress_state

FROM --platform=${TARGETPLATFORM} docker.io/alpine

ARG TARGETARCH

COPY --from=builder /opt/synapse-compressor/synapse_compress_state /usr/local/bin/synapse_compress_state