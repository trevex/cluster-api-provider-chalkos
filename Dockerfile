# Builds the bootstrap or the control-plane manager, selected by --build-arg package=bootstrap
# or --build-arg package=controlplane.
FROM golang:1.27 AS builder
ARG TARGETOS
ARG TARGETARCH
ARG package

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o manager ./${package}

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
