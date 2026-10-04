# syntax=docker/dockerfile:1.26.0@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
FROM registry.suse.com/bci/golang:1.26@sha256:ba19f36b38a37f72e8c1b418daa3f27a67f5a80f822cb79aef9ecf73a05e4fe6 AS builder

WORKDIR /app

# Copy the build script and source code
COPY . /app

# Make the build script executable
RUN chmod +x /app/scripts/build

# Run the build script
RUN /app/scripts/build

FROM registry.suse.com/bci/bci-base:15.7@sha256:b39dcf02d6ef54bd2cba185060d41435c75d87fff0f94ce0a2b73f2a24e5e66d

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
    iputils telnet qemu-tools iproute2 e2fsprogs xfsprogs && \
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
