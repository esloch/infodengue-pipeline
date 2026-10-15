FROM golang:1.24.8-alpine3.22 AS builder

ARG MINIO_VERSION=RELEASE.2025-10-15T17-29-55Z
ARG MINIO_COMMIT=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a

RUN apk add --no-cache git \
    && git clone --filter=blob:none https://github.com/minio/minio.git /src \
    && cd /src \
    && git checkout --detach "${MINIO_COMMIT}" \
    && test "$(git describe --tags --exact-match HEAD)" = "${MINIO_VERSION}" \
    && mkdir -p /out \
    && CGO_ENABLED=0 MINIO_RELEASE=RELEASE go build \
      -tags kqueue \
      -trimpath \
      --ldflags "$(MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go)" \
      -o /out/minio .

FROM alpine:3.22.1

RUN apk add --no-cache ca-certificates curl

COPY --from=builder /out/minio /usr/local/bin/minio

VOLUME ["/data"]
EXPOSE 9000 9001

ENTRYPOINT ["minio"]
