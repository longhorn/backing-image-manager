FROM registry.suse.com/bci/bci-base:15.4

ARG ARCH=amd64

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/network:/utilities/SLE_15_SP4/network:utilities.repo && \
    zypper --gpg-auto-import-keys ref

RUN zypper -n install kmod curl wget nfs-client nfs4-acl-tools fuse \
	librdmacm1 librdmacm-utils libibverbs perl-Config-General libaio-devel sg3_utils \
     iputils telnet iperf qemu-tools iproute2 e2fsprogs e2fsprogs-devel xfsprogs xfsprogs-devel

COPY bin/backing-image-manager /usr/local/bin/

VOLUME /usr/local/bin

# Add Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["backing-image-manager"]
