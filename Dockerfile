# syntax=docker/dockerfile:1.23.0@sha256:2780b5c3bab67f1f76c781860de469442999ed1a0d7992a5efdf2cffc0e3d769
FROM golangci/golangci-lint:v2.12.2@sha256:5cceeef04e53efe1470638d4b4b4f5ceefd574955ab3941b2d9a68a8c9ad5240 AS golangci-lint

FROM registry.suse.com/bci/golang:1.26@sha256:2445a685ad302627a2c6b75f0915a37bc1277f2dc65572bad70723f78ca9dfb2 AS base

ARG TARGETARCH
ARG http_proxy
ARG https_proxy

ENV ARCH=${TARGETARCH}
ENV GOFLAGS=-mod=vendor

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/openSUSE_Factory/ snappy && \
    zypper --gpg-auto-import-keys ref

# Install packages
RUN zypper -n install cmake wget curl git less file \
    libkmod-devel libnl3-devel linux-glibc-devel pkg-config psmisc fuse \
    librdmacm1 librdmacm-utils libibverbs qemu-tools \
    perl-Config-General libaio-devel glibc-devel-static glibc-devel iptables libltdl7 libdevmapper1_03 iproute2 jq gcc && \
    rm -rf /var/cache/zypp/*

# Copy golangci-lint binary from official image
COPY --from=golangci-lint /usr/bin/golangci-lint /usr/local/bin/golangci-lint

WORKDIR /go/src/github.com/longhorn/backing-image-manager
COPY . .

FROM base AS build
RUN ./scripts/build

FROM base AS validate
RUN ./scripts/validate && touch /validate.done

FROM base AS test
RUN ./scripts/test

FROM scratch AS build-artifacts
COPY --from=build /go/src/github.com/longhorn/backing-image-manager/bin/ /bin/

FROM scratch AS test-artifacts
COPY --from=test /go/src/github.com/longhorn/backing-image-manager/coverage.out /coverage.out

FROM scratch AS ci-artifacts
COPY --from=build /go/src/github.com/longhorn/backing-image-manager/bin/ /bin/
COPY --from=validate /validate.done /validate.done
COPY --from=test /go/src/github.com/longhorn/backing-image-manager/coverage.out /coverage.out
