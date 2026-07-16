# syntax=docker/dockerfile:1.20.0@sha256:26147acbda4f14c5add9946e2fd2ed543fc402884fd75146bd342a7f6271dc1d
FROM registry.suse.com/bci/golang:1.25@sha256:bd17be73bd89df69fa98dc085afdaee4edbe7cd97d15c6b52f48089ce234088e AS builder

WORKDIR /app

# Copy the build script and source code
COPY . /app

# Make the build script executable
RUN chmod +x /app/scripts/build

# Run the build script
RUN /app/scripts/build

FROM registry.suse.com/bci/bci-base:16.1@sha256:6ae795e896f4a7e1e77e3ea84335c9572e3c64253256f297945cf45d08b3ce20

ARG TARGETPLATFORM
RUN if [ "$TARGETPLATFORM" != "linux/amd64" ] && [ "$TARGETPLATFORM" != "linux/arm64" ]; then \
    echo "Error: Unsupported TARGETPLATFORM: $TARGETPLATFORM" && \
    exit 1; \
    fi

ENV ARCH ${TARGETPLATFORM#linux/}

RUN zypper --gpg-auto-import-keys ref && \
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
