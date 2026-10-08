#!/usr/bin/env bash
# Smoke test of a built brrrrr image (CI runs it before every push of the image):
# - every example pipeline validates inside the image (the binary runs on its base image);
# - checkpoints go to the volume by default: with no --checkpoints the image's user can write its
#   checkpoint directory, in the image and on a volume mounted at /var/lib/brrrrr as a Kubernetes
#   PVC is (empty and root-owned): refused, naming fsGroup, without the pod's fsGroup; written with
#   it;
# - `extended` (the -extended image, built with FEATURES=object-store): TLS to an object store
#   works, an S3 request with a made-up key must reach S3 and be refused for that key (without CA
#   certificates it fails in the TLS handshake instead). Otherwise an s3:// URL is refused, naming
#   the -extended image.
# - `historical` (the -historical image, FEATURES=historical): `brrrrr historical` is there.
set -euo pipefail
image="${1:?usage: image-smoke.sh <image> [extended|iggy|historical]}"
extended="${2:-}"
here="$(cd "$(dirname "$0")/.." && pwd)"
for f in "$here"/fixtures/pipelines/*.sql; do
  docker run --rm -v "$here:/src:ro" "$image" validate "/src/fixtures/pipelines/$(basename "$f")" > /dev/null
done
echo "validate: every example pipeline compiles in $image"
read -ra extra <<< "${DOCKER_ARGS:-}" # e.g. network/proxy settings for a local run
run=(run /src/tests/acceptance/sql/windows.sql --proto /src/fixtures/market.proto --brokers 127.0.0.1:1
  --metrics 127.0.0.1:0)

# the default checkpoint directory: brrrrr opens it (and writes a probe file) before it contacts any
# broker, then goes on to fail on the missing broker (a run is stopped after a while: with no broker
# it would only give up after a minute)
volume="brrrrr-smoke-$$" name="brrrrr-smoke-$$"
trap 'docker rm -f "$name" > /dev/null 2>&1; docker volume rm -f "$volume" > /dev/null' EXIT
start() { # the run's output; the arguments go to `docker run`
  timeout 15 docker run --name "$name" -v "$here:/src:ro" "$@" "${extra[@]}" "$image" "${run[@]}" 2>&1 || true
  docker rm -f "$name" > /dev/null 2>&1 || true
}
opened="checkpoints: /var/lib/brrrrr/checkpoints/windows/"
fail() { # what went wrong, then the run's output
  echo "checkpoints: $1:" >&2
  tail -c 2000 <<< "$2" >&2
  exit 1
}

# in the image, with no volume mounted: the Dockerfile's chown makes it the image user's
out=$(start)
grep -q "$opened" <<< "$out" || fail "the default directory in the image did not open" "$out"

# on a volume as Kubernetes provisions a PVC: empty and root-owned. With the pod's
# securityContext.fsGroup, the kubelet gives the volume's root that group (group-writable,
# setgid) and the container that supplementary group; without it, the image's user cannot write
# there. `nocopy`: on its first mount Docker fills a new named volume from the image's directory,
# contents and ownership, which a PVC does not get.
fsgroup=65534 # the fsGroup the image documents (Dockerfile)
pvc() { # a fresh volume, its root prepared as root (by the image's own shell) with $1
  docker volume rm -f "$volume" > /dev/null
  docker volume create "$volume" > /dev/null
  docker run --rm --user 0 --entrypoint sh -v "$volume:/v:nocopy" "$image" -c "$1"
}
mount=(-v "$volume:/var/lib/brrrrr:nocopy")
pvc "chown 0:0 /v && chmod 755 /v"
out=$(start "${mount[@]}")
refused="checkpoint directory /var/lib/brrrrr/checkpoints/windows/.* is not writable (on Kubernetes: .*securityContext.fsGroup"
if grep -q "$opened" <<< "$out" || ! grep -q "$refused" <<< "$out"; then
  fail "a root-owned volume without fsGroup was not refused naming fsGroup" "$out"
fi
pvc "chown 0:$fsgroup /v && chmod 2775 /v"
out=$(start "${mount[@]}" --group-add "$fsgroup")
grep -q "$opened" <<< "$out" || fail "a root-owned volume with fsGroup $fsgroup did not open" "$out"
echo "checkpoints: /var/lib/brrrrr/checkpoints opens in the image, and on a PVC-like volume with fsGroup $fsgroup (without it: refused, naming fsGroup)"

s3=(-e AWS_ACCESS_KEY_ID=AKIASMOKETEST000000 -e AWS_SECRET_ACCESS_KEY=smoke -e AWS_REGION=us-east-1)
out=$(timeout 60 docker run --name "$name" -v "$here:/src:ro" "${extra[@]}" "${s3[@]}" "$image" "${run[@]}" \
  --checkpoints s3://brrrrr-image-smoke-test/checkpoints 2>&1 || true)
docker rm -f "$name" > /dev/null 2>&1 || true
if [ "$extended" = extended ]; then
  if ! grep -q InvalidAccessKeyId <<< "$out"; then
    echo "TLS: the S3 request did not get through to S3:" >&2
    tail -c 2000 <<< "$out" >&2
    exit 1
  fi
  echo "TLS: S3 reached with verified certificates"
else
  if ! grep -q "without the object-store feature" <<< "$out"; then
    echo "object store: an s3:// URL was not refused by the image without the feature:" >&2
    tail -c 2000 <<< "$out" >&2
    exit 1
  fi
  echo "object store: s3:// refused, pointing at the -extended image"
fi

if [ "$extended" = historical ]; then
  # the help captured first: `grep -q` leaves at its first match, and the rest of the help then
  # breaks the pipe, which pipefail would report as the command missing
  help=$(docker run --rm "$image" historical --help)
  grep -q -- "--format" <<< "$help" || { echo "historical: brrrrr historical is not in $image" >&2; exit 1; }
  echo "historical: brrrrr historical runs in $image"
fi
if [ "$extended" = iggy ]; then
  out=$(docker run --rm -v "$here:/src:ro" "$image" "${run[@]}" --iggy invalid 2>&1 || true)
  if ! grep -q -- "--iggy requires iggy+tcp://" <<< "$out"; then
    echo "Iggy: the image does not enable its source feature:" >&2
    tail -c 2000 <<< "$out" >&2
    exit 1
  fi
  echo "Iggy: source feature enabled"
fi
