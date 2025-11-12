# TODO: get nfs support working with distroless.
#
# Note that it's not enough to just copy over the mount commands,
# there are other system services that must be started as well.
#
# TODO: create a new container to replace the nfs-server container.

ARG VERSION

FROM gke.gcr.io/debian-base:bookworm-v1.0.6-gke.2@sha256:14264b090b8f98d640e59c85496947061ca68c00fbb092e4a3ce8e42e6ec1cef AS debian
# Note that netbase is needed to get rpcbind working correctly.
RUN clean-install \
  mount bash nfs-common netbase

FROM google-go.pkg.dev/golang:1.24.3@sha256:5d2e3331f2ea940f316408b516badb01b146c918abe6941e3154c4704ba50140 AS builder
WORKDIR /src
COPY . .
RUN go build -ldflags "-extldflags=static -X main.driverVersion=$VERSION" ./cmd/multitier_driver

FROM debian AS driver
COPY --from=builder /src/multitier_driver /

COPY --from=builder /src/cmd/multitier_driver/startup.sh /
ENTRYPOINT ["/startup.sh"]
