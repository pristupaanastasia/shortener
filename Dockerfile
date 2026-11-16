# Both binaries in one image; the Deployment chooses the command.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Static binaries: the runtime image has no libc.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/shortener ./cmd/shortener \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/consumer ./cmd/consumer

# distroless: no shell or package manager, runs as an unprivileged user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/shortener /out/consumer /usr/local/bin/
# Numeric, so Kubernetes can verify runAsNonRoot (65532 is distroless's nonroot).
USER 65532:65532
EXPOSE 8080 9091
ENTRYPOINT ["/usr/local/bin/shortener"]
