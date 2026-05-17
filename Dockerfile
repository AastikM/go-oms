# Stage 1: Build 
# Use official Go image — gives us the full toolchain
FROM golang:1.22-alpine AS builder

# Install git (needed for go get with some modules)
RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy dependency files first — Docker caches this layer.
# If go.mod/go.sum don't change, this layer is reused on rebuild.
# This is the key Docker build optimisation for Go: deps first, code second.
COPY go.mod go.sum ./
RUN go mod download

# Now copy source and build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-w -s" -o /oms-server ./cmd/server

# -w  strips DWARF debug info  → smaller binary
# -s  strips symbol table      → smaller binary
# CGO_ENABLED=0                → fully static binary, no libc dependency


# Stage 2: Runtime 
# Distroless image — no shell, no package manager, minimal attack surface.
# Just the binary and the CA certs (needed for HTTPS to Yahoo Finance).
FROM gcr.io/distroless/static-debian12

COPY --from=builder /oms-server /oms-server
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# OMS listens on 8080
EXPOSE 8080

# Read config from environment variables
# These are set in docker-compose.yml
ENTRYPOINT ["/oms-server"]
CMD [ \
    "--redis",   "localhost:6379", \
    "--pg-host", "localhost", \
    "--pg-user", "oms_user", \
    "--pg-pass", "oms_pass", \
    "--pg-db",   "oms_db", \
    "--sim",     "true" \
]
