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

Default `pytest` stays broker-free. Optional broker tests:

```bash
pytest -m integration
```
