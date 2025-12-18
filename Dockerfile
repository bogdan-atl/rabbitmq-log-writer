FROM golang:1.22-alpine AS builder

WORKDIR /src
RUN apk add --no-cache ca-certificates git

# 先只拷 go.mod 用于缓存依赖
COPY go.mod ./
RUN go mod download

# 再拷源码，然后生成完整 go.sum（包含传递依赖）
COPY . .
RUN go mod tidy

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/udp-logger ./cmd/udp-logger

FROM alpine:3.20

RUN apk add --no-cache ca-certificates && adduser -D -H -u 10001 app
USER 10001:10001

WORKDIR /app
COPY --from=builder /out/udp-logger /app/udp-logger

EXPOSE 516/udp
ENTRYPOINT ["/app/udp-logger"]



