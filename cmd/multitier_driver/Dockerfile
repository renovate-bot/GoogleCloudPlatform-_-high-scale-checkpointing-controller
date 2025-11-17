# TODO: get nfs support working with distroless.
#
# Note that it's not enough to just copy over the mount commands,
# there are other system services that must be started as well.
#
# TODO: create a new container to replace the nfs-server container.

ARG VERSION

FROM gke.gcr.io/debian-base:bookworm-v1.0.6-gke.2 AS debian
# Note that netbase is needed to get rpcbind working correctly.
RUN clean-install \
  mount bash nfs-common netbase

FROM --platform=$BUILDPLATFORM google-go.pkg.dev/golang:1.24.3 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags "-extldflags=static -X main.driverVersion=$VERSION" \
    ./cmd/multitier_driver

FROM debian AS driver
COPY --from=builder /src/multitier_driver /

COPY --from=builder /src/cmd/multitier_driver/startup.sh /
ENTRYPOINT ["/startup.sh"]
