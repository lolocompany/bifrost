FROM golang:1.26-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/bifrost ./cmd/bifrost

FROM alpine:3.22

RUN apk add --no-cache ca-certificates

RUN addgroup -S bifrost && adduser -S -G bifrost bifrost
USER bifrost

WORKDIR /app
COPY --from=build /out/bifrost /usr/local/bin/bifrost

ENTRYPOINT ["/usr/local/bin/bifrost"]
