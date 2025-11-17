FROM --platform=$BUILDPLATFORM golang:1.24 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    ./cmd/multitier_stub

FROM gke.gcr.io/debian-base:bookworm-v1.0.4-gke.17 AS debian
RUN clean-install \
  bash \
  procps file xxd lsof iputils-ping dnsutils # all for debugging

COPY --from=builder /src/multitier_stub /usr/bin/

CMD ["sleep", "36000"]
