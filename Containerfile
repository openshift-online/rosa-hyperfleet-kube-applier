FROM registry.access.redhat.com/ubi9/go-toolset:9.8-1792671270 AS builder

USER root

WORKDIR /app
COPY go.mod go.sum ./
COPY seaplane-dynamo/go.mod ./seaplane-dynamo/go.mod
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOFIPS140=certified GOEXPERIMENT=strictfipsruntime \
    go build -tags no_openssl -o /kube-applier-aws .

FROM registry.access.redhat.com/ubi9/ubi-minimal:9.8-1791898448
COPY --from=builder /kube-applier-aws /kube-applier-aws
USER 65532:65532
ENTRYPOINT ["/kube-applier-aws"]
