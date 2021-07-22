FROM ubuntu:20.04

ARG ARCH=amd64

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get install -y kmod curl wget nfs-common fuse \
    libibverbs1 librdmacm1 libconfig-general-perl libaio1 sg3-utils \
    iputils-ping telnet iperf qemu-utils iproute2 && \
    rm -rf /var/lib/apt/lists/*

COPY bin/backing-image-manager /usr/local/bin/

VOLUME /usr/local/bin

# Add Tini
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["backing-image-manager"]
