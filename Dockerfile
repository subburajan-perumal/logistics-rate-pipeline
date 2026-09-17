# syntax=docker/dockerfile:1
# Multi-stage build for both binaries (docs/PLAN.md D-21).
#   docker build --target ingestd     -t ingestd:dev .
#   docker build --target mocksources -t mocksources:dev .
FROM golang:1.27-alpine AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12:nonroot AS ingestd
LABEL org.opencontainers.image.source="https://github.com/subburajan-perumal/logistics-rate-pipeline"
COPY --from=build /out/ingestd /ingestd
COPY recipes /etc/recipes
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/ingestd"]
CMD ["serve"]

FROM gcr.io/distroless/static-debian12:nonroot AS mocksources
LABEL org.opencontainers.image.source="https://github.com/subburajan-perumal/logistics-rate-pipeline"
COPY --from=build /out/mocksources /mocksources
USER nonroot:nonroot
EXPOSE 8081
ENTRYPOINT ["/mocksources"]
CMD ["serve"]
