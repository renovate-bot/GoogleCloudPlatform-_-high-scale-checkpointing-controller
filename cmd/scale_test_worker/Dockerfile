FROM --platform=$BUILDPLATFORM golang:1.24 as builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags -extldflags=static ./cmd/scale_test_worker

FROM gke.gcr.io/debian-base:bookworm-v1.0.4-gke.17
COPY --from=builder /src/scale_test_worker /
ENTRYPOINT ["/scale_test_worker"]
