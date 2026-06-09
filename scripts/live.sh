#!/usr/bin/env bash
# Live mode: stream events stamped at the current time so rows land in real time
# during the demo. Runs until you press Ctrl-C.
#
# Defaults: unlimited events, ~500/s.
# Override with env vars or pass extra flags through to produce.py, e.g.:
#   RATE=2000 ./scripts/live.sh
#   COUNT=100000 ./scripts/live.sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR/producer"

PY="python3"
[[ -x .venv/bin/python ]] && PY=".venv/bin/python"

exec "$PY" produce.py \
  --count "${COUNT:-0}" \
  --rate "${RATE:-500}" \
  "$@"
