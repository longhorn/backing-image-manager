# syntax=docker/dockerfile:1.24.0@sha256:87999aa3d42bdc6bea60565083ee17e86d1f3339802f543c0d03998580f9cb89

ARG GOLANGCI_LINT_VERSION=v2.12.2
FROM golangci/golangci-lint:${GOLANGCI_LINT_VERSION} AS golangci-lint

FROM registry.suse.com/bci/golang:1.26@sha256:f413accb043d80ea904e972958c06543e5fa3225652b29b44c51f027246b3b81 AS base

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
