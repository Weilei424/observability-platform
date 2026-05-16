# Stage 1: build
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Download dependencies before copying source so this layer caches independently.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a statically linked binary.
COPY . .
RUN CGO_ENABLED=0 go build -o /server ./cmd/server

# Stage 2: minimal runtime image
FROM gcr.io/distroless/static-debian12

# Run as nonroot (uid 65532 is the distroless convention).
USER 65532:65532

COPY --from=builder /server /server

EXPOSE 8080
ENTRYPOINT ["/server"]
