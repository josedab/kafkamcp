# Build stage
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kafkamcp ./cmd/kafkamcp

# Runtime stage
FROM alpine:3.24

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /out/kafkamcp /usr/local/bin/kafkamcp

# Create non-root user
RUN addgroup -g 1000 kafkamcp && \
    adduser -u 1000 -G kafkamcp -D kafkamcp

USER kafkamcp

ENTRYPOINT ["/usr/local/bin/kafkamcp"]
CMD ["--config", "/etc/kafkamcp/kafkamcp.yaml"]
