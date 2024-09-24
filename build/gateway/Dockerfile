FROM swr.cn-north-4.myhuaweicloud.com/cloud-native-riscv64/golang:1.19 AS builder

ARG GO_LDFLAGS
ARG TARGETARCH

WORKDIR /code
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH GO_LDFLAGS=$GO_LDFLAGS make WHAT=edgemesh-gateway


FROM swr.cn-north-4.myhuaweicloud.com/cloud-native-riscv64/alpine:edge

COPY --from=builder /code/_output/local/bin/edgemesh-gateway /usr/local/bin/edgemesh-gateway

ENTRYPOINT ["edgemesh-gateway"]
