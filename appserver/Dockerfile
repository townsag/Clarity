# syntax=docker/dockerfile:1
# build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY appserver.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o appserver .

# Running Stage
FROM alpine:latest

# install CA certifs
RUN apk --no-cache add ca-certificates

FROM gcr.io/distroless/base-debian12 AS build-release-stage

WORKDIR /app

COPY --from=builder /app/appserver .

RUN adduser -D appuser && \
    chown -R appuser:appuser /app

USER appuser

EXPOSE 8080

CMD ["./appserver"]