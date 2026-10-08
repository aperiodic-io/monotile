# The brrrrr image: the release binary on debian slim. It links only libc, libm, libgcc_s and
# zlib: OpenSSL is built into librdkafka, jemalloc (the allocator) into the binary. BUILDER can
# point at a local toolchain image to avoid a pull. Checkpoints go to /var/lib/brrrrr/checkpoints:
# mount the pod's volume (a Kubernetes PVC) at /var/lib/brrrrr, with the pod's
# securityContext.fsGroup set to 65534.
#
# FEATURES=object-store builds the `-extended` image, which can also checkpoint to an object
# store (s3://, or an S3-compatible one) through object_store and rustls with the system CA certificates.
# FEATURES=iggy builds the `-iggy` image with Apache Iggy sources and Kafka sinks.
# CARGO_ARGS= (empty) builds the public image of a release (release.yml): with the default `sql`
# feature (`brrrrr sql` and `brrrrr serve`), which the pipeline images leave out.
#
# The dependencies build in a layer of their own (cargo-chef): a change to brrrrr's code rebuilds
# only brrrrr, and CI keeps that layer in its cache (ci.yml, the image jobs).
ARG BUILDER=rust:1.95.0-bookworm
FROM ${BUILDER} AS chef
WORKDIR /src
# the pinned toolchain and its components, before anything is built with it
COPY rust-toolchain.toml .
RUN cargo install --locked cargo-chef@0.1.78

FROM chef AS plan
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
ARG FEATURES=""
ARG CARGO_ARGS="--no-default-features"
COPY --from=plan /src/recipe.json recipe.json
RUN cargo chef cook --release --locked -p brrrrr ${CARGO_ARGS} --features "${FEATURES}" --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked -p brrrrr ${CARGO_ARGS} --features "${FEATURES}" && strip target/release/brrrrr

FROM debian:bookworm-slim
# system roots for TLS: to the Kafka brokers (librdkafka) and, in -extended, the object store
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# the checkpoints' default place, writable by the image's user when no volume is mounted there
RUN mkdir -p /var/lib/brrrrr/checkpoints && chown -R 65534:65534 /var/lib/brrrrr
COPY --from=build /src/target/release/brrrrr /usr/local/bin/brrrrr
# remote files are cached here: the image's user has no home, nor may `--user` (any uid can write /tmp)
ENV BRRRRR_CACHE=/tmp/brrrrr-cache
USER 65534
# 9464: a pipeline's metrics; 4242 and 5433: `brrrrr serve`
EXPOSE 9464 4242 5433
ENTRYPOINT ["brrrrr"]
