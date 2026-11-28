FROM mirror.gcr.io/library/debian:trixie-slim@sha256:8aa5a7bf5a1955bddbe7d74fcdd0b91ab30e74362a8b73af672b0481b35b44a0 AS netem-tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates ethtool iproute2 iputils-ping \
    && rm -rf /var/lib/apt/lists/*

FROM rust:1.99.1-bookworm@sha256:e6f2cff35d0f309acf8433b4f05878ed77092602c5065cc81801177da0bb3a9c AS build
WORKDIR /src
COPY . .
ARG HTRIO_LAB_REVISION=unrecorded
RUN HTRIO_LAB_REVISION="$HTRIO_LAB_REVISION" cargo build --locked -j 2 --release \
    --example bbr_lab --no-default-features --features http3,json

FROM netem-tools AS lab
COPY --from=build /src/target/release/examples/bbr_lab /usr/local/bin/bbr_lab
CMD ["sleep", "infinity"]
