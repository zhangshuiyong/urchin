ARG BASE_IMAGE=debian:bullseye

FROM golang:1.19-bullseye as builder

ARG GOPROXY
ARG GOTAGS
ARG GOGCFLAGS

WORKDIR /go/src/openi/urchin/v2

COPY . /go/src/openi/urchin/v2

RUN go env -w GOPROXY=https://goproxy.cn,direct

RUN go mod tidy

RUN make build-dfget && make install-dfget

FROM ${BASE_IMAGE} as health

ENV GRPC_HEALTH_PROBE_VERSION v0.4.11
RUN mkdir -p /grpc/${GRPC_HEALTH_PROBE_VERSION}
COPY ./grpc-ecosystem-grpc-health-probe/${GRPC_HEALTH_PROBE_VERSION} /grpc/${GRPC_HEALTH_PROBE_VERSION}

RUN if [ "$(uname -m)" = "ppc64le" ]; then \
    cp /grpc/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-ppc64le /bin/grpc_health_probe; \
    elif [ "$(uname -m)" = "aarch64" ]; then \
    cp /grpc/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-arm64 /bin/grpc_health_probe; \
    else \
    cp /grpc/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 /bin/grpc_health_probe; \
    fi && \
    chmod +x /bin/grpc_health_probe

FROM ${BASE_IMAGE}

ENV PATH=/opt/urchin/bin:$PATH

RUN apt-get -qq update \
    && apt-get -qq install -y --no-install-recommends ca-certificates curl

RUN echo "hosts: files dns" > /etc/nsswitch.conf

COPY --from=builder /opt/dragonfly/bin/dfget /opt/urchin/bin/dfget
COPY --from=health /bin/grpc_health_probe /bin/grpc_health_probe

EXPOSE 65001

ENTRYPOINT ["/opt/urchin/bin/dfget", "daemon"]

