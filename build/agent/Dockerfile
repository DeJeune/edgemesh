FROM swr.cn-north-4.myhuaweicloud.com/cloud-native-riscv64/golang:1.19 AS builder

ARG GO_LDFLAGS
ARG TARGETARCH

WORKDIR /code

# Copy the entire repository
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH GO_LDFLAGS=$GO_LDFLAGS make WHAT=edgemesh-agent

FROM swr.cn-north-4.myhuaweicloud.com/cloud-native-riscv64/alpine:edge

# 添加中国的Alpine镜像源
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apk/repositories

# Install iptables and other necessary packages
RUN apk update && \
    apk add --no-cache iptables iptables-legacy dpkg bash

COPY --from=builder /code/_output/local/bin/edgemesh-agent /usr/local/bin/edgemesh-agent

# Add support for auto-detection of iptables mode
COPY --from=builder /code/build/agent/iptables-wrapper /sbin/iptables-wrapper
RUN update-alternatives --install /sbin/iptables iptables /sbin/iptables-legacy 50
RUN update-alternatives --install /sbin/iptables iptables /sbin/iptables-nft 50
RUN update-alternatives --install /sbin/iptables iptables /sbin/iptables-wrapper 100 \
    --slave /sbin/iptables-restore iptables-restore /sbin/iptables-wrapper \
    --slave /sbin/iptables-save iptables-save /sbin/iptables-wrapper

ENTRYPOINT ["edgemesh-agent"]
