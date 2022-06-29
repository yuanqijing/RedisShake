# Build the manager binary
FROM golang:1.17-alpine as builder

WORKDIR /go/src/redis-shake
RUN apk add --update make git bash rsync gcc musl-dev

# Copy the go source
# Copy the Go Modules manifests
COPY src/go.mod go.mod
COPY src/go.sum go.sum

# Copy the go dependencies
COPY src/vendor/ vendor/

# Copy the go source
COPY src/cmd/ cmd/
COPY src/pkg/ pkg/
COPY src/redis-shake/ redis-shake/

# Build
ENV GOOS linux
ENV GOARCH amd64
RUN go build -a -v -o redis-shake-linux /go/src/redis-shake/cmd/redis-shake/*.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM alpine:3.12
RUN apk add --update bash net-tools iproute2 logrotate less rsync util-linux
WORKDIR /
COPY --from=builder /go/src/redis-shake/redis-shake-linux ./redis-shake

CMD ["redis-shake"]

# configmap should mount on /redis-shake.conf
ENTRYPOINT ["-type=sync", "-conf=redis-shake.conf"]
