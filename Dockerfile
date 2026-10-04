FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY . .
COPY --from=agentic_api / /agentic-api
RUN go work init . /agentic-api && go mod download
RUN CGO_ENABLED=0 go build -tags=nomsgpack -trimpath -ldflags="-s -w" -o /worker ./cmd/worker
FROM scratch
COPY --from=build /worker /worker
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
USER 65532:65532
ENV GIN_MODE=release
ENTRYPOINT ["/worker"]
