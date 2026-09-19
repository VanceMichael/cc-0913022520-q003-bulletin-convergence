FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN GOPROXY=https://goproxy.cn,https://proxy.golang.org,direct go mod download
COPY . .
RUN CGO_ENABLED=0 go build -mod=readonly -o /out/server ./cmd/server \
 && CGO_ENABLED=0 go build -mod=readonly -o /out/subscriber-sim ./cmd/subscriber-sim

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates wget \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/server /usr/local/bin/server
COPY --from=build /out/subscriber-sim /usr/local/bin/subscriber-sim
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/server"]
