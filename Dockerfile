FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN GOPROXY=https://goproxy.cn,direct go mod download
COPY . .
RUN CGO_ENABLED=0 go build -mod=readonly -trimpath -ldflags="-s -w" -o /out/service ./cmd/server

FROM debian:bookworm-slim
# 投递器会对订阅方发起 HTTPS 回调，运行时需要 CA 根证书。
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/service /service
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/service"]
