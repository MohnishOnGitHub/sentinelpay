#!/usr/bin/env bash
# End-to-end Phase 1C smoke test. Requires Docker.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PYTHON="${PYTHON:-$ROOT/.venv/bin/python}"
if [[ ! -x "$PYTHON" ]]; then
  PYTHON="python3"
fi

echo "==> Starting local Kafka (KRaft)"
docker compose up -d --wait

echo "==> Publishing 10 synthetic events"
"$PYTHON" -m producer.app --count 10 --seed 42 --rate 2

echo "==> Inspecting transactions.raw"
"$PYTHON" -m producer.inspect_topic --max-messages 10 --timeout 30

echo "==> Smoke test passed"
