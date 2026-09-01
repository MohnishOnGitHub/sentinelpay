# SentinelPay

## Local Kafka Demo

Phase 1C runs a single-node KRaft Kafka broker (no ZooKeeper) for local development.

Start the broker:

```bash
docker compose up
```

Kafka listens on `localhost:9092`, matching `KAFKA_BOOTSTRAP_SERVERS`.
The compose stack creates `transactions.raw`, `transactions.validated`, and
`transactions.dlq` (1 partition, replication factor 1).
Auto-create is also enabled. To create a topic by hand:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic transactions.raw \
  --partitions 1 \
  --replication-factor 1
```

In a second terminal, publish 10 synthetic events:

```bash
python -m producer.app --count 10 --seed 42 --rate 2
```

In a third terminal, print them:

```bash
python -m producer.inspect_topic --max-messages 10
```

Expected result: 10 JSON transaction events from `transactions.raw`, each with an
`account_id` key and `schema_version`.

One-shot equivalent (broker must already be up, or use the script which starts it):

```bash
./scripts/smoke_kafka.sh
```

### Validation demo

With Kafka running, start the validator:

```bash
python -m validation.service
```

Publish valid events:

```bash
python -m producer.app --count 5 --seed 42 --rate 2
```

Confirm they land on `transactions.validated`:

```bash
python -m producer.inspect_topic --topic transactions.validated --max-messages 5
```

Publish one invalid event and confirm the DLQ:

```bash
python -m producer.publish_invalid --case amount
python -m producer.inspect_topic --topic transactions.dlq --max-messages 1
```

`--case` can also be `json`, `enum`, `extra`, `coords`, or `missing`.

One-shot equivalent:

```bash
./scripts/smoke_validation.sh
```

### Spark streaming demo (Phase 2A / 2B)

PySpark runs in local mode. A JDK 11 or 17 must be installed and `JAVA_HOME` set.
There is no Spark cluster in this phase.

```bash
# Terminal 1
docker compose up -d

# Terminal 2
python -m validation.service

# Terminal 3
python -m streaming.main

# Terminal 4
python -m producer.app --count 20 --seed 42 --rate 5
```

Spark prints tumbling event-time account windows to the console. Rows use a
`window_size` of `5m` or `30m`; the columns below map to `*_5m` / `*_30m`
feature names.

One-shot equivalent (broker, validator, and Spark):

```bash
./scripts/smoke_streaming.sh
```

Default `pytest` stays broker-free. Spark tests run locally when Java is available.
Optional broker tests:

```bash
pytest -m integration
```

### Behavioral features (Phase 2B)

All temporal features use `event_timestamp` (UTC), not processing time.
Watermark default is 10 minutes; checkpoint default is `.checkpoints/streaming`.
Override with `SPARK_WATERMARK` and `SPARK_CHECKPOINT_DIR`.

Windows are **tumbling**, not sliding. A transaction at 10:04 UTC contributes
only to the 10:00–10:05 and 10:00–10:30 windows. Sliding windows would multiply
per-key state without giving true per-transaction lookbacks; those need keyed
state later (`applyInPandasWithState` / mapGroupsWithState).

#### Window-level account features

| Column | Meaning |
|---|---|
| `txn_count` | Distinct-after-dedup transactions in the window |
| `amount_sum` / `amount_avg` / `amount_max` | `DECIMAL` amount aggregates |
| `unique_merchants` | Distinct `merchant_id` values |
| `unique_devices` | Distinct `device_id` values |
| `unique_locations` | Distinct lat/lon grid cells (default 3 decimal places, ~100 m) |
| `location_spread_km` | Haversine length of the window's lat/lon bounding-box diagonal |
| `high_amount_count` | Transactions with `amount >= HIGH_AMOUNT_THRESHOLD` |

#### Transaction-level signals

| Signal | Grain | Meaning |
|---|---|---|
| `is_high_amount` | transaction | `amount >= HIGH_AMOUNT_THRESHOLD` (stateless). Rolled into `high_amount_count` on each window. |

#### Window-level risk signals

These are behavioral flags, not fraud decisions.

| Signal | Meaning |
|---|---|
| `multi_device_signal` | `unique_devices >= MULTI_DEVICE_THRESHOLD` (default 2) in this window. Not a lifetime "new device" flag. |
| `rapid_transaction_signal` | `txn_count >= RAPID_TXN_COUNT_THRESHOLD` (default 5). Same cutoff for 5m and 30m. |
| `rapid_location_change_signal` | At least two grid cells **and** `location_spread_km >= LOCATION_SPREAD_KM_THRESHOLD` (default 25). Not impossible-travel. |

Thresholds are centralized in `streaming/config.py` / `FeatureConfig` and can be
overridden with `HIGH_AMOUNT_THRESHOLD`, `RAPID_TXN_COUNT_THRESHOLD`,
`MULTI_DEVICE_THRESHOLD`, `LOCATION_SPREAD_KM_THRESHOLD`, and
`LOCATION_GRID_DECIMALS`.

#### Current limitations

- No lifetime device history, so there is no `new_device_for_account` feature.
- `location_spread_km` uses the bounding-box diagonal, not max pairwise distance
  and not consecutive-transaction speed. Impossible-travel is not implemented.
- Window features are not joined back onto individual transactions (that would
  be a streaming-to-streaming join or keyed state).
- The 24h velocity windows from the PRD are not in this phase.
- Amount z-scores, merchant fraud rates, and model scores are out of scope.
