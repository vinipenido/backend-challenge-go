# syntax=docker/dockerfile:1
# Build multi-stage. A versão do Go é a mesma declarada em go.mod (1.27.1).
ARG GO_VERSION=1.27.1

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
# Dependências primeiro, para aproveitar o cache de camadas.
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/wallet-service ./cmd/wallet-service \
 && go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S app && adduser -S -G app app
WORKDIR /app
COPY --from=build /out/wallet-service /out/migrate /app/
USER app
EXPOSE 8080
# STOPSIGNAL padrão (SIGTERM) dispara o shutdown ordenado do Fx.
ENTRYPOINT ["/app/wallet-service"]
