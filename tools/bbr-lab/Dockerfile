FROM mirror.gcr.io/library/debian:trixie-slim@sha256:8aa5a7bf5a1955bddbe7d74fcdd0b91ab30e74362a8b73af672b0481b35b44a0 AS netem-tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates ethtool iproute2 iputils-ping \
    && rm -rf /var/lib/apt/lists/*

FROM rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS build
WORKDIR /src
COPY . .
ARG HTRIO_LAB_REVISION=unrecorded
RUN HTRIO_LAB_REVISION="$HTRIO_LAB_REVISION" cargo build --locked -j 2 --release \
    --example bbr_lab --no-default-features --features http3,json

FROM netem-tools AS lab
COPY --from=build /src/target/release/examples/bbr_lab /usr/local/bin/bbr_lab
CMD ["sleep", "infinity"]
