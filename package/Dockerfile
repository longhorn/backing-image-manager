# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b
FROM registry.suse.com/bci/golang:1.27@sha256:aab9e6c4d40ccb3b465c2d32d328b6c55740c1aeb3e4c1eb4cd380e00b222601 AS builder

WORKDIR /app

# Copy the build script and source code
COPY . /app

# Make the build script executable
RUN chmod +x /app/scripts/build

# Run the build script
RUN /app/scripts/build

FROM registry.suse.com/bci/bci-base:15.7@sha256:8cd100ae2ec5ab23d6ec7aa1c0fe2dc30ca852c609406097797c9d298b7decdd

ARG TARGETPLATFORM
RUN if [ "$TARGETPLATFORM" != "linux/amd64" ] && [ "$TARGETPLATFORM" != "linux/arm64" ]; then \
    echo "Error: Unsupported TARGETPLATFORM: $TARGETPLATFORM" && \
    exit 1; \
    fi

ENV ARCH ${TARGETPLATFORM#linux/}

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/openSUSE_Factory/system:snappy.repo && \
    zypper --gpg-auto-import-keys ref && \
    zypper -n ref && \
    zypper update -y && \
    zypper -n clean --all

RUN zypper -n install kmod curl wget nfs-client nfs4-acl-tools fuse \
    librdmacm1 librdmacm-utils libibverbs perl-Config-General libaio1 sg3_utils \
    iputils telnet qemu-tools iproute2 e2fsprogs xfsprogs cifs-utils && \
    zypper -n clean --all

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
    zypper update -y && \
    zypper -n clean --all

RUN zypper -n install iperf && \
    zypper -n clean --all

COPY --chmod=0755 --from=builder /app/bin/backing-image-manager-${ARCH} /usr/local/bin/backing-image-manager

VOLUME /usr/local/bin

# Add Tini
ENV TINI_VERSION v0.19.0
ADD --chmod=0755 https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
ENTRYPOINT ["/tini", "--"]

CMD ["backing-image-manager"]
