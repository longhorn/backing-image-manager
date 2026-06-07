# syntax=docker/dockerfile:1.23.0@sha256:2780b5c3bab67f1f76c781860de469442999ed1a0d7992a5efdf2cffc0e3d769
FROM registry.suse.com/bci/golang:1.26@sha256:f413accb043d80ea904e972958c06543e5fa3225652b29b44c51f027246b3b81 AS builder

WORKDIR /app

# Copy the build script and source code
COPY . /app

# Make the build script executable
RUN chmod +x /app/scripts/build

# Run the build script
RUN /app/scripts/build

FROM registry.suse.com/bci/bci-base:15.7@sha256:85510dded17d5049b12bb5ffbab74fe34325815f507ad85faf315c95ff5d1c64

ARG TARGETPLATFORM
RUN if [ "$TARGETPLATFORM" != "linux/amd64" ] && [ "$TARGETPLATFORM" != "linux/arm64" ]; then \
    echo "Error: Unsupported TARGETPLATFORM: $TARGETPLATFORM" && \
    exit 1; \
    fi

ENV ARCH ${TARGETPLATFORM#linux/}

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/openSUSE_Factory/system:snappy.repo && \
    zypper --gpg-auto-import-keys ref && \
    zypper -n ref && \
    zypper update -y

RUN zypper -n install kmod curl wget nfs-client nfs4-acl-tools fuse \
    librdmacm1 librdmacm-utils libibverbs perl-Config-General libaio-devel sg3_utils \
    iputils telnet qemu-tools iproute2 e2fsprogs e2fsprogs-devel xfsprogs xfsprogs-devel

RUN if [ "$ARCH" = "amd64" ]; then \
        REPO_URL="https://download.opensuse.org/repositories/network:utilities/openSUSE_Factory/network:utilities.repo"; \
    elif [ "$ARCH" = "arm64" ]; then \
        REPO_URL="https://download.opensuse.org/repositories/network:utilities/openSUSE_Factory_ARM/network:utilities.repo"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    zypper -n addrepo --refresh "$REPO_URL" && \
    zypper --gpg-auto-import-keys ref && \
    zypper -n ref && \
    zypper update -y

RUN zypper -n install iperf

COPY --from=builder /app/bin/backing-image-manager-${ARCH} /usr/local/bin/backing-image-manager
RUN chmod +x /usr/local/bin/backing-image-manager

VOLUME /usr/local/bin

# Add Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["backing-image-manager"]
