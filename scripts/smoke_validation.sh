#!/usr/bin/env bash
# Phase 1D validation smoke test. Requires Docker + a running or startable broker.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PYTHON="${PYTHON:-$ROOT/.venv/bin/python}"
if [[ ! -x "$PYTHON" ]]; then
  PYTHON="python3"
fi
export PYTHONUNBUFFERED=1

echo "==> Starting local Kafka (KRaft)"
docker compose up -d
for _ in $(seq 1 40); do
  if docker inspect --format '{{.State.Health.Status}}' sentinelpay-kafka 2>/dev/null | grep -q healthy; then
    break
  fi
  sleep 2
done

echo "==> Starting validator (6 messages)"
"$PYTHON" -m validation.service --max-messages 6 --timeout 45 &
VALIDATOR_PID=$!
sleep 3

echo "==> Publishing 5 valid events"
"$PYTHON" -m producer.app --count 5 --seed 42 --rate 2

echo "==> Publishing 1 invalid event"
"$PYTHON" -m producer.publish_invalid --case amount

wait "$VALIDATOR_PID"

echo "==> Inspecting transactions.validated"
"$PYTHON" -m producer.inspect_topic --topic transactions.validated --max-messages 5 --timeout 30

echo "==> Inspecting transactions.dlq"
"$PYTHON" -m producer.inspect_topic --topic transactions.dlq --max-messages 1 --timeout 30

echo "==> Validation smoke test passed"
