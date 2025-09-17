# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install git for go mod download
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o syncsvc .

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /home/appuser/

# Create non-root user and home directory
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup && \
    mkdir -p /home/appuser && \
    chown appuser:appgroup /home/appuser

# Copy binary and config with proper ownership
COPY --from=builder --chown=appuser:appgroup /app/syncsvc ./
COPY --from=builder --chown=appuser:appgroup /app/config.json* ./

USER appuser

# Default environment variables
ENV CONFIG_PATH=./config.json
ENV BATCH_SIZE=1000
ENV DRY_RUN=false

# Run the binary
CMD ["./syncsvc"]