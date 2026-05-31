#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}"

export BALDA_NATS_EMBEDDED="${BALDA_NATS_EMBEDDED:-true}"
export BALDA_NATS_HOST="${BALDA_NATS_HOST:-127.0.0.1}"
export BALDA_NATS_PORT="${BALDA_NATS_PORT:-4222}"

exec go run ./cmd/balda start "$@"
