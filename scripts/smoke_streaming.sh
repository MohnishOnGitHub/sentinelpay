#!/usr/bin/env bash
# Phase 3A Kafka → validator → Spark → Parquet smoke.
# Requires Docker, Java 11/17, and PySpark.
#
# Gold is append-mode: a 5m window at 10:00–10:05 UTC is written only after
# the watermark (10 minutes) passes window_end. The late wave at 10:50 UTC
# is published after Spark has started so that can happen in a later batch.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PYTHON="${PYTHON:-$ROOT/.venv/bin/python}"
if [[ ! -x "$PYTHON" ]]; then
  PYTHON="python3"
fi
export PYTHONUNBUFFERED=1
export SMOKE_RUN_ID="$$"
export DATA_LAKE_DIR=".smoke-lake"
export SILVER_CHECKPOINT_DIR=".checkpoints/smoke-silver"
export GOLD_CHECKPOINT_DIR=".checkpoints/smoke-gold"
export SPARK_CHECKPOINT_DIR=".checkpoints/smoke-console"
export SPARK_CONSOLE_SINK="${SPARK_CONSOLE_SINK:-true}"

echo "==> Starting local Kafka (KRaft)"
docker compose up -d
for _ in $(seq 1 40); do
  if docker inspect --format '{{.State.Health.Status}}' sentinelpay-kafka 2>/dev/null | grep -q healthy; then
    break
  fi
  sleep 2
done

echo "==> Resetting smoke lake and checkpoints"
rm -rf "$DATA_LAKE_DIR" "$SILVER_CHECKPOINT_DIR" "$GOLD_CHECKPOINT_DIR" "$SPARK_CHECKPOINT_DIR"

echo "==> Starting validator"
"$PYTHON" -m validation.service --timeout 90 &
VALIDATOR_PID=$!
sleep 3

echo "==> Starting Spark streaming (80s)"
"$PYTHON" -m streaming.main --timeout-seconds 80 &
SPARK_PID=$!
sleep 18

echo "==> Publishing early event-time wave (10:01 / 10:03 UTC)"
"$PYTHON" -m producer.publish_timed --wave early --run-id "$SMOKE_RUN_ID"
sleep 12

echo "==> Publishing late event-time wave (10:50 / 10:52 UTC) to close Gold windows"
"$PYTHON" -m producer.publish_timed --wave late --run-id "$SMOKE_RUN_ID"

wait "$VALIDATOR_PID"
wait "$SPARK_PID"

echo "==> Reading persisted Parquet"
"$PYTHON" -m streaming.inspect_lake --require-gold
echo "==> Spark → Parquet smoke finished"
