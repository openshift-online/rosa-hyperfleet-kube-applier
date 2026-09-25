FROM registry.access.redhat.com/ubi8/go-toolset:9.8-1790174511 AS builder

USER root

WORKDIR /app
COPY go.mod go.sum ./
COPY hyperfleet-dynamo/go.mod ./hyperfleet-dynamo/go.mod
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /kube-applier-aws .

FROM registry.access.redhat.com/ubi9/ubi-minimal:9.8-1790074235
COPY --from=builder /kube-applier-aws /kube-applier-aws
USER 65532:65532
ENTRYPOINT ["/kube-applier-aws"]
