# syntax=docker/dockerfile:1.17.1@sha256:38387523653efa0039f8e1c89bb74a30504e76ee9f565e25c9a09841f9427b05
FROM registry.suse.com/bci/golang:1.25@sha256:feeb8b4f66de0a8ba333ee6a3f0e97b4cefbaf638b2873b29fb270ab5ca8722b AS builder

WORKDIR /app

# Copy the build script and source code
COPY . /app

# Make the build script executable
RUN chmod +x /app/scripts/build

# Run the build script
RUN /app/scripts/build

FROM registry.suse.com/bci/bci-base:15.7@sha256:3267acc7b9744218250e91e1d2d3176a113d88c4c55158945beb46150c0c7df2

ARG TARGETPLATFORM
RUN if [ "$TARGETPLATFORM" != "linux/amd64" ] && [ "$TARGETPLATFORM" != "linux/arm64" ]; then \
    echo "Error: Unsupported TARGETPLATFORM: $TARGETPLATFORM" && \
    exit 1; \
    fi

ENV ARCH ${TARGETPLATFORM#linux/}

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/network:/utilities/15.6/network:utilities.repo && \
    zypper --gpg-auto-import-keys ref && \
    zypper -n ref && \
    zypper update -y

RUN zypper -n install kmod curl wget nfs-client nfs4-acl-tools fuse \
    librdmacm1 librdmacm-utils libibverbs perl-Config-General libaio-devel sg3_utils \
    iputils telnet iperf qemu-tools iproute2 e2fsprogs e2fsprogs-devel xfsprogs xfsprogs-devel

COPY --from=builder /app/bin/backing-image-manager-${ARCH} /usr/local/bin/backing-image-manager
RUN chmod +x /usr/local/bin/backing-image-manager

VOLUME /usr/local/bin

# Add Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["backing-image-manager"]
