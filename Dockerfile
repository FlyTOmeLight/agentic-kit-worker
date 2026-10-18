# syntax=docker/dockerfile:1
FROM golang:1.27.1-alpine AS build
RUN apk add --no-cache git
ENV GOPRIVATE=github.com/FlyTOmeLight/*
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=secret,id=github_netrc,target=/root/.netrc,required=true go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -tags=nomsgpack -trimpath -ldflags="-s -w" -o /worker ./cmd/worker
FROM scratch
COPY --from=build /worker /worker
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY conf /conf
USER 65532:65532
ENV LISTEN_ADDR=0.0.0.0:8081 LOG_CONFIG_FILE=/conf/server/logger.toml
EXPOSE 8081
ENTRYPOINT ["/worker"]
