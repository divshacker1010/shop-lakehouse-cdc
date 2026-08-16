# syntax=docker/dockerfile:1
# MinIO server + client, built from official source.
# MinIO discontinued pre-built Docker image distribution (Oct 2025, see
# https://github.com/minio/minio/issues/21647) and moved to source-only
# distribution. This Dockerfile builds from their actual source tree,
# using the same version-stamping flags their own release pipeline used.
# Build recipe adapted from https://github.com/coollabsio/minio (build
# automation only -- MinIO itself remains AGPLv3).

ARG MINIO_VERSION=latest
ARG TARGETARCH

FROM golang:1.24-alpine AS builder
ARG MINIO_VERSION
ARG TARGETARCH
ENV GOPATH=/go
ENV CGO_ENABLED=0
WORKDIR /workspace

RUN apk add -U --no-cache ca-certificates git make curl bash && \
    go install aead.dev/minisign/cmd/minisign@v0.2.1

RUN if [ "$MINIO_VERSION" = "latest" ]; then \
        git clone --depth 1 https://github.com/minio/minio.git .; \
    else \
        git clone --depth 1 --branch ${MINIO_VERSION} https://github.com/minio/minio.git .; \
    fi

RUN COMMIT_ID=$(git rev-parse HEAD) && \
    SHORT_COMMIT_ID=$(git rev-parse --short=12 HEAD) && \
    COMMIT_TIME=$(git show -s --format=%ci HEAD) && \
    RELEASE_TAG=$(git describe --tags --exact-match 2>/dev/null || echo "${MINIO_VERSION}") && \
    if [ "$RELEASE_TAG" = "latest" ]; then \
        RELEASE_TAG="DEVELOPMENT.$(date -u +%Y-%m-%dT%H-%M-%SZ)"; \
    fi && \
    VERSION_TIME=$(echo ${RELEASE_TAG} | sed 's/RELEASE\.//' | sed 's/T/ /' | sed 's/-/:/3' | sed 's/-/:/3' | sed 's/ /T/') && \
    COPYRIGHT_YEAR=$(date -u +%Y) && \
    CGO_ENABLED=0 go build -trimpath \
        -ldflags "-s -w \
        -X github.com/minio/minio/cmd.Version=${VERSION_TIME} \
        -X github.com/minio/minio/cmd.ReleaseTag=${RELEASE_TAG} \
        -X github.com/minio/minio/cmd.CommitID=${COMMIT_ID} \
        -X github.com/minio/minio/cmd.ShortCommitID=${SHORT_COMMIT_ID} \
        -X github.com/minio/minio/cmd.CopyrightYear=${COPYRIGHT_YEAR}" \
        -o /usr/bin/minio .

RUN /usr/bin/minio --version

# Download the mc client and verify its signature against MinIO's real
# public signing key before trusting it -- don't skip this step.
RUN echo "TARGETARCH is: '${TARGETARCH}'"

RUN curl -fsSL https://dl.min.io/aistor/mc/release/linux-${TARGETARCH}/mc -o /usr/bin/mc && \
    curl -fsSL https://dl.min.io/aistor/mc/release/linux-${TARGETARCH}/mc.minisig -o /usr/bin/mc.minisig && \
    chmod +x /usr/bin/mc
RUN ls -la /usr/bin/mc /usr/bin/mc.minisig
RUN /go/bin/minisign -Vqm /usr/bin/mc -x /usr/bin/mc.minisig -P RWTx5Zr1tiHQLwG9keckT0c45M3AGeHD6IvimQHpyRywVWGbP1aVSGav
RUN /usr/bin/mc --version

FROM registry.access.redhat.com/ubi9/ubi-micro:latest
LABEL org.opencontainers.image.source="https://github.com/minio/minio" \
      org.opencontainers.image.licenses="AGPL-3.0"

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/bin/minio* /usr/bin/
COPY --from=builder /usr/bin/mc* /usr/bin/
COPY --from=builder /workspace/dockerscripts/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh
RUN chmod -R 755 /usr/bin

EXPOSE 9000 9001
VOLUME ["/data"]
ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]
CMD ["minio"]