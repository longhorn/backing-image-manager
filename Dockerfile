# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b

FROM golangci/golangci-lint:v2.15.0@sha256:fbfcfd12a6f8d7f1b97b3362ca80fc2a244d46863148c73cb88e7793af96c481 AS golangci-lint

FROM registry.suse.com/bci/golang:1.27@sha256:634cd6f1e373bcde01a1f8df6d1c3a9784142353a0842b5371d6385b4e9d2abb AS base

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
