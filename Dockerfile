# Multi-stage build for optimal size and security
FROM golang:1.23-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -a -installsuffix cgo \
    -ldflags "-s -w -extldflags '-static'" \
    -o datax \
    ./cmd/datax

# Final stage - minimal runtime image
FROM scratch

# Copy CA certificates for SSL/TLS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the binary
COPY --from=builder /app/datax /usr/local/bin/datax

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/datax"]

# Default command
CMD ["--help"]

# Metadata
LABEL org.opencontainers.image.title="DataX"
LABEL org.opencontainers.image.description="High-performance data synchronization tool written in Go"
LABEL org.opencontainers.image.vendor="longkeyy"
LABEL org.opencontainers.image.source="https://github.com/longkeyy/go-datax"
LABEL org.opencontainers.image.documentation="https://github.com/longkeyy/go-datax/blob/main/README.md"
LABEL org.opencontainers.image.licenses="MIT"