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
docker compose up -d
for _ in $(seq 1 40); do
  if docker inspect --format '{{.State.Health.Status}}' sentinelpay-kafka 2>/dev/null | grep -q healthy; then
    break
  fi
  sleep 2
done

echo "==> Publishing 10 synthetic events"
"$PYTHON" -m producer.app --count 10 --seed 42 --rate 2

echo "==> Inspecting transactions.raw"
"$PYTHON" -m producer.inspect_topic --max-messages 10 --timeout 30

echo "==> Smoke test passed"
