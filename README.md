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

### Spark streaming demo (Phase 2A)

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

Spark prints account window aggregates to the console:

- `account_id`
- `window_start` / `window_end` (event time)
- `window_size` (`5m` or `30m`)
- `txn_count` / `amount_sum`

These map to `txn_count_5m`, `amount_sum_5m`, `txn_count_30m`, and `amount_sum_30m`.
Watermark default is 10 minutes; checkpoint default is `.checkpoints/streaming`.
Override with `SPARK_WATERMARK` and `SPARK_CHECKPOINT_DIR`.

Default `pytest` stays broker-free. Spark tests run locally when Java is available.
Optional broker tests:

```bash
pytest -m integration
```
