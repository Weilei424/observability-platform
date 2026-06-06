# Stage 1: build
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Download dependencies before copying source so this layer caches independently.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build both binaries.
COPY . .
RUN CGO_ENABLED=0 go build -o /server ./cmd/server && \
    CGO_ENABLED=0 go build -o /loadgen ./examples/load-generator && \
    mkdir -p /data

# Stage 2: backend runtime image
FROM gcr.io/distroless/static-debian12 AS backend

# Run as nonroot (uid 65532 is the distroless convention).
USER 65532:65532

COPY --from=builder /server /server
COPY --from=builder --chown=65532:65532 /data /data

EXPOSE 8080
ENTRYPOINT ["/server"]

# Stage 3: load generator runtime image
FROM gcr.io/distroless/static-debian12 AS loadgen

USER 65532:65532

COPY --from=builder /loadgen /loadgen

ENTRYPOINT ["/loadgen"]
