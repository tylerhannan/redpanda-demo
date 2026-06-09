#!/usr/bin/env bash
# Create the Redpanda topic used by this demo with rpk.
#
# Prereqs:
#   - rpk installed (https://docs.redpanda.com/current/get-started/rpk-install/)
#   - .env filled in (see .env.example)
#
# Usage: ./scripts/setup_topic.sh
set -euo pipefail

# Load .env from repo root.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
else
  echo "No .env found at $ROOT_DIR/.env. Copy .env.example and fill it in." >&2
  exit 1
fi

: "${REDPANDA_BROKERS:?set REDPANDA_BROKERS in .env}"
: "${REDPANDA_USERNAME:?set REDPANDA_USERNAME in .env}"
: "${REDPANDA_PASSWORD:?set REDPANDA_PASSWORD in .env}"
TOPIC="${REDPANDA_TOPIC:-clickstream_events}"
MECH="${REDPANDA_SASL_MECHANISM:-SCRAM-SHA-256}"

echo "Creating topic '$TOPIC' on $REDPANDA_BROKERS ..."
rpk topic create "$TOPIC" \
  --brokers "$REDPANDA_BROKERS" \
  --user "$REDPANDA_USERNAME" \
  --password "$REDPANDA_PASSWORD" \
  --sasl-mechanism "$MECH" \
  --tls-enabled \
  --partitions 6

echo "Done. Topics:"
rpk topic list \
  --brokers "$REDPANDA_BROKERS" \
  --user "$REDPANDA_USERNAME" \
  --password "$REDPANDA_PASSWORD" \
  --sasl-mechanism "$MECH" \
  --tls-enabled
