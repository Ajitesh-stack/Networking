# Stage 1: Build Go binaries
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Copy go.mod first to leverage Docker layer caching
COPY go.mod ./
RUN go mod download

# Copy the rest of the workspace source code
COPY . .

# Build statically linked binaries with debugging symbols stripped to minimize image size
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/server ./server
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/generator ./generator

# Stage 2: Lightweight execution environment
FROM alpine:latest

WORKDIR /app

# Copy compiled binaries from builder stage
COPY --from=builder /bin/server /usr/local/bin/server
COPY --from=builder /bin/generator /usr/local/bin/generator

# Copy mobility dataset files (train.csv and test.csv)
COPY --from=builder /src/generator/data /app/generator/data

# Default to running the ingestion server. 
# To run the generator instead, simply override the CMD when running:
# "docker run spatial-ingestion-server generator"
CMD ["server"]
