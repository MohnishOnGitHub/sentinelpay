#!/usr/bin/env bash
# Phase 2B Spark streaming smoke. Requires Docker, Java 11/17, and PySpark.
# Console output should include unique_merchants, unique_devices, location_spread_km,
# and the window-level risk signals in addition to txn_count / amount_*.
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

echo "==> Starting validator"
"$PYTHON" -m validation.service --max-messages 20 --timeout 60 &
VALIDATOR_PID=$!
sleep 3

echo "==> Starting Spark streaming (45s)"
rm -rf .checkpoints/streaming-smoke
SPARK_CHECKPOINT_DIR=".checkpoints/streaming-smoke" \
  "$PYTHON" -m streaming.main --timeout-seconds 45 &
SPARK_PID=$!
sleep 15

echo "==> Publishing 20 valid events"
"$PYTHON" -m producer.app --count 20 --seed 42 --rate 5

wait "$VALIDATOR_PID"
wait "$SPARK_PID"
echo "==> Spark streaming smoke finished"
