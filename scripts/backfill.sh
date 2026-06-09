#!/usr/bin/env bash
# Backfill mode: bulk-load a large batch of events whose event_time is spread
# across the last N days, to build up a realistic history before a demo.
#
# Defaults: 5,000,000 events, ~20k/s, spread over 7 days.
# Override with env vars or pass extra flags through to produce.py, e.g.:
#   COUNT=10000000 BACKFILL_DAYS=14 ./scripts/backfill.sh
#   ./scripts/backfill.sh --topic other_topic
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR/producer"

PY="python3"
[[ -x .venv/bin/python ]] && PY=".venv/bin/python"

exec "$PY" produce.py \
  --count "${COUNT:-5000000}" \
  --rate "${RATE:-20000}" \
  --backfill-days "${BACKFILL_DAYS:-7}" \
  "$@"
