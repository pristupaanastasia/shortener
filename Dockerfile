# Both binaries in one image; the Deployment chooses the command.
# Pinned: go.mod needs Go 1.26.3, and digests make builds reproducible.
FROM golang:1.26.3@sha256:2d6c80227255c3112a4d08e67ba98e58efd3846daf15d9d7d4c389565d881b1a AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Static binaries: the runtime image has no libc.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/shortener ./cmd/shortener \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/consumer ./cmd/consumer

# distroless: no shell or package manager, runs as an unprivileged user.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/shortener /out/consumer /usr/local/bin/
# Numeric, so Kubernetes can verify runAsNonRoot (65532 is distroless's nonroot).
USER 65532:65532
EXPOSE 8080 9091
ENTRYPOINT ["/usr/local/bin/shortener"]
