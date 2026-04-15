# syntax=docker/dockerfile:1
#
# Release image for GoReleaser dockers_v2: copies the pre-built Linux binary from the
# build context (see https://goreleaser.com/customization/package/dockers_v2/).
#
# Local builds without GoReleaser: use Dockerfile.build in the repository root.

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
    && addgroup -S bifrost && adduser -S -G bifrost bifrost

USER bifrost

WORKDIR /home/bifrost

ARG VERSION=unknown
ARG REVISION=unknown
ARG BUILD_TIME=unknown
ARG TARGETPLATFORM

LABEL org.opencontainers.image.title="Bifrost"
LABEL org.opencontainers.image.description="A configurable Kafka cluster bridge for moving records between topics and clusters using declarative YAML configurations."
LABEL org.opencontainers.image.source="https://github.com/lolocompany/bifrost"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.vendor="Lolo Company"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.revision="${REVISION}"
LABEL org.opencontainers.image.created="${BUILD_TIME}"

COPY --chmod=0755 ${TARGETPLATFORM}/bifrost /usr/local/bin/bifrost

ENTRYPOINT ["/usr/local/bin/bifrost"]
