#!/usr/bin/env bash
# Run a command in a resource-capped Rust container (1 CPU by default).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
read -ra extra <<< "${DOCKER_ARGS:-}" # extra `docker run` arguments, split on whitespace
docker run --rm --cpus="${CPUS:-1}" --memory="${MEMORY:-3g}" --network="${NETWORK:-bridge}" \
  -v "$here":/src -w /src -v brrrrr-cargo:/usr/local/cargo/registry -v brrrrr-target:/src/target \
  -v /var/run/docker.sock:/var/run/docker.sock -e CARGO_TERM_COLOR=never "${extra[@]}" \
  "${IMAGE:-l2live-rust}" "$@"
