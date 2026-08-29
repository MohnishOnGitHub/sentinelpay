# DESIGN — Real-Time Payment Risk & Fraud Intelligence Platform

**Project codename:** SentinelPay  
**Document type:** Technical Design Specification  
**Version:** v1.0

---

## 1. Design Objectives

The architecture should demonstrate realistic design decisions for a real-time payment risk platform while remaining feasible to build as an individual portfolio project.

The system should optimize for:

- Clear separation of responsibilities
- Event-driven processing
- Replayability
- Explainability
- Data correctness
- Reproducibility
- Low-cost local development
- Progressive cloud adoption

---

## 2. High-Level Architecture

```text
                         ┌───────────────────────────────┐
                         │ Synthetic / Historical Events │
                         └───────────────┬───────────────┘
                                         │
                                         ▼
                               ┌──────────────────┐
                               │ Kafka Producer   │
                               └────────┬─────────┘
                                        │
                                        ▼
                              transactions.raw
                                        │
                         ┌──────────────┴──────────────┐
                         │                             │
                         ▼                             ▼
                Schema Validator                Raw S3 Sink
                         │                        Bronze
                ┌────────┴─────────┐
                │                  │
                ▼                  ▼
       transactions.validated   transactions.dlq
                │
                ▼
      Spark Structured Streaming
                │
        ┌───────┼───────────────────┐
        │       │                   │
        ▼       ▼                   ▼
   Dedup    Enrichment      Streaming Features
        │                           │
        └──────────────┬────────────┘
                       ▼
                 Feature Vector
                       │
             ┌─────────┴──────────┐
             │                    │
             ▼                    ▼
        Rules Engine            ML Model
             │                    │
             └─────────┬──────────┘
                       ▼
                 Risk Aggregator
                       │
        ┌──────────────┼─────────────────┐
        │              │                 │
        ▼              ▼                 ▼
   fraud.alerts    PostgreSQL       S3 Silver/Gold
        │
        ▼
   Dashboard / API

ML training path:

S3 Gold / Historical Data
        │
        ▼
 Feature Dataset
        │
        ▼
 Model Training
        │
        ▼
 MLflow Tracking + Registry
        │
        ▼
 Approved Model Artifact
        │
        ▼
 Streaming Inference
```

---

## 3. Technology Choices

| Layer | Technology | Rationale |
|---|---|---|
| Language | Python | Strong ecosystem for DE + DS |
| Streaming broker | Apache Kafka | Durable event log, replay, partitioning, consumer groups |
| Stream processing | Spark Structured Streaming | Stateful event-time processing, windows, watermarks |
| Operational DB | PostgreSQL | Durable operational state and analytics familiarity |
| Object storage | AWS S3 | Standard cloud data-lake storage |
| Catalog/query | Glue + Athena | Low-cost serverless analytics |
| Model training | scikit-learn + XGBoost/LightGBM | Strong baseline + production-standard tree model |
| Experiment tracking | MLflow | Model lifecycle and reproducibility |
| Validation | Great Expectations | Explicit data-quality contracts |
| Containers | Docker / Docker Compose | Repeatable local environment |
| API | FastAPI | Lightweight Python API |
| Dashboard | Streamlit initially | Fast portfolio visualization |
| Monitoring | Prometheus/Grafana stretch | Production-style observability |

---

## 4. Repository Structure

```text
sentinelpay/
├── README.md
├── PRD.md
├── DESIGN.md
├── docker-compose.yml
├── .env.example
├── Makefile
│
├── producer/
│   ├── app.py
│   ├── generator.py
│   ├── schemas.py
│   └── config.py
│
├── streaming/
│   ├── main.py
│   ├── validation.py
│   ├── enrichment.py
│   ├── deduplication.py
│   ├── windows.py
│   ├── features/
│   │   ├── velocity.py
│   │   ├── amount.py
│   │   ├── geography.py
│   │   ├── device.py
│   │   └── merchant.py
│   └── sinks/
│       ├── postgres.py
│       └── s3.py
│
├── risk/
│   ├── rules.py
│   ├── scorer.py
│   ├── reason_codes.py
│   └── thresholds.py
│
├── ml/
│   ├── train.py
│   ├── evaluate.py
│   ├── feature_pipeline.py
│   ├── model_registry.py
│   └── notebooks/
│
├── api/
│   ├── main.py
│   ├── routes/
│   └── schemas.py
│
├── dashboard/
│   └── app.py
│
├── data_quality/
│   └── expectations/
│
├── infra/
│   ├── aws/
│   └── terraform/        # stretch
│
├── scripts/
│   ├── seed_data.py
│   ├── replay.py
│   └── benchmark.py
│
└── tests/
    ├── unit/
    ├── integration/
    └── load/
```

---

## 5. Event Schema

### Transaction Event v1

```json
{
  "schema_version": 1,
  "transaction_id": "txn_001",
  "account_id": "acct_1001",
  "event_timestamp": "2026-08-29T10:15:12.183Z",
  "amount": 1299.00,
  "currency": "INR",
  "merchant_id": "m_340",
  "merchant_category": "ELECTRONICS",
  "device_id": "dev_005",
  "latitude": 12.9716,
  "longitude": 77.5946,
  "country": "IN",
  "channel": "ECOMMERCE",
  "transaction_type": "PURCHASE"
}
```

### Partitioning Key

Kafka key:

```text
account_id
```

Rationale:

- Preserves per-account ordering
- Allows account-level velocity and stateful computations
- Distributes accounts across partitions

Trade-off:

A single extremely active account may create a hotspot, but this is acceptable for the project.

---

## 6. Kafka Design

### Topics

#### `transactions.raw`

Purpose:
- Immutable source stream

Retention:
- Long enough to support replay during development

Key:
- `account_id`

#### `transactions.validated`

Purpose:
- Schema-valid transactions

Key:
- `account_id`

#### `fraud.alerts`

Purpose:
- REVIEW/BLOCK decisions

Key:
- `account_id`

#### `transactions.dlq`

Purpose:
- Invalid or unprocessable messages

Key:
- Original key where available

DLQ message includes:

- original payload
- error reason
- processing timestamp
- validator version

---

## 7. Consumer Groups

Potential groups:

```text
validator-group
stream-feature-group
s3-bronze-group
alert-persistence-group
```

Independent consumer groups allow downstream consumers to process the same event stream without coupling.

---

## 8. Event-Time Processing

Fraud features must use:

```text
event_timestamp
```

rather than arrival/processing time.

Spark should apply a watermark, for example:

```text
10 minutes
```

This supports late-arriving events without retaining unlimited state.

Example:

```text
Event created: 10:01
Arrives:       10:06
```

The event should still contribute to the correct event-time window.

---

## 9. Deduplication

Unique key:

```text
transaction_id
```

Spark must remove duplicates within the watermark horizon.

Persistent sinks should also enforce uniqueness.

PostgreSQL:

```sql
PRIMARY KEY (transaction_id)
```

Writes should use an idempotent upsert strategy.

This protects against:

- producer retries
- Kafka redelivery
- consumer restart
- duplicated input data

---

## 10. Feature Engineering Design

## 10.1 Velocity

State grouped by:

```text
account_id
```

Windows:

- 5 minutes
- 30 minutes
- 24 hours

Features:

```text
txn_count_5m
txn_count_30m
txn_count_24h
amount_sum_5m
amount_sum_30m
amount_sum_24h
```

---

## 10.2 Amount Deviation

Historical account aggregates:

```text
account_mean_amount
account_std_amount
```

Feature:

```text
amount_zscore =
(amount - account_mean_amount)
/
max(account_std_amount, epsilon)
```

Also compute:

- ratio to rolling median
- percentile within account history

---

## 10.3 Geographic Risk

Use haversine distance between consecutive transaction coordinates.

```text
distance_km
time_delta_hours

implied_speed =
distance_km / time_delta_hours
```

Potential rule:

```text
if implied_speed > 900 km/h
and distance > 500 km
then IMPOSSIBLE_TRAVEL
```

Threshold must be configurable.

---

## 10.4 Device Risk

Features:

```text
new_device_for_account
device_seen_count_30d
device_account_count
device_txn_count_1h
```

A device associated with multiple unrelated accounts may receive a higher risk signal.

---

## 10.5 Merchant Risk

Features:

```text
merchant_historical_fraud_rate
merchant_txn_count
merchant_avg_amount
new_merchant_for_account
merchant_category_risk
```

Avoid target leakage by calculating merchant fraud-rate features only from prior historical observations.

---

## 10.6 Temporal Behavior

Features:

- hour_of_day
- day_of_week
- weekend
- unusual_hour_for_account
- seconds_since_previous_txn

---

## 11. Data Leakage Controls

This is critical.

Feature generation must not use information unavailable at transaction decision time.

Examples of prohibited leakage:

- Future transactions
- Final fraud label for current transaction
- Merchant fraud statistics containing future labels
- Account aggregates computed using future transactions

All historical aggregates must be point-in-time correct.

---

## 12. Rules Engine

Rules produce:

```text
risk contribution
reason code
severity
```

Example:

```python
{
    "reason_code": "IMPOSSIBLE_TRAVEL",
    "risk_delta": 0.25,
    "severity": "HIGH"
}
```

Candidate rules:

- IMPOSSIBLE_TRAVEL
- HIGH_TXN_VELOCITY
- EXTREME_AMOUNT_DEVIATION
- NEW_DEVICE_HIGH_VALUE
- HIGH_RISK_MERCHANT
- RAPID_COUNTRY_CHANGE
- DEVICE_SHARED_ACROSS_ACCOUNTS

Rules must be configurable, not embedded throughout pipeline logic.

---

## 13. ML Design

### Baseline

Logistic Regression.

Purpose:
- Establish interpretable baseline.

### Main Model

XGBoost or LightGBM.

Reasons:
- Strong tabular performance
- Handles non-linear interactions
- Robust industry baseline
- Fast inference

### Unsupervised Comparison

Isolation Forest.

Purpose:
- Demonstrate anomaly-detection comparison
- Explore detection of previously unseen fraud patterns

---

## 14. Handling Class Imbalance

Potential techniques:

- class weights
- scale_pos_weight
- undersampling of majority class
- threshold optimization

SMOTE may be tested but should not automatically be assumed best because synthetic observations can distort transaction feature relationships.

---

## 15. Model Metrics

Primary:

```text
PR-AUC
Recall
Precision
F1
```

Secondary:

```text
ROC-AUC
False-positive rate
False-negative rate
```

Business-oriented evaluation:

```text
Expected fraud loss prevented
Cost of false declines
```

Potential objective:

```text
expected_cost =
fraud_missed * fraud_cost
+
legitimate_blocked * false_decline_cost
```

Decision threshold can then be optimized for expected cost rather than raw accuracy.

---

## 16. Risk Aggregation

The initial implementation can combine:

```text
ML probability
+
rules risk contribution
```

Example:

```text
final_score =
min(
    1.0,
    0.75 * model_probability
    + rule_risk_total
)
```

This is intentionally simple and transparent for v1.

Later iterations may learn the combination.

---

## 17. Decision Policy

Configuration:

```yaml
thresholds:
  review: 0.50
  block: 0.80
```

Policy:

```text
score < 0.50
→ APPROVE

0.50 <= score < 0.80
→ REVIEW

score >= 0.80
→ BLOCK
```

---

## 18. PostgreSQL Schema

### transactions

```sql
transaction_id      TEXT PRIMARY KEY
account_id          TEXT NOT NULL
event_timestamp     TIMESTAMPTZ NOT NULL
amount              NUMERIC NOT NULL
currency            TEXT NOT NULL
merchant_id         TEXT
device_id           TEXT
risk_score          DOUBLE PRECISION
decision            TEXT
model_version       TEXT
processed_at         TIMESTAMPTZ
```

### fraud_alerts

```sql
alert_id             UUID PRIMARY KEY
transaction_id       TEXT UNIQUE
risk_score           DOUBLE PRECISION
decision             TEXT
reason_codes         JSONB
status               TEXT
created_at           TIMESTAMPTZ
reviewed_at          TIMESTAMPTZ
```

---

## 19. S3 Medallion Design

Example path layout:

```text
s3://sentinelpay-data/
  bronze/
    transactions/
      year=2026/month=08/day=29/hour=12/

  silver/
    transactions/
      year=2026/month=08/day=29/

  gold/
    fraud_features/
    risk_decisions/
    merchant_metrics/
    model_monitoring/
```

Storage format:

```text
Parquet
```

Compression:

```text
Snappy
```

Partitioning should be coarse enough to avoid the small-file problem.

---

## 20. Data Quality

Great Expectations or equivalent checks:

### Transaction-Level

- transaction_id not null
- account_id not null
- amount > 0
- currency in allowed set
- valid lat/lon
- event_timestamp not in unrealistic future
- transaction_id uniqueness

### Pipeline-Level

- expected event volume range
- fraud-score bounds 0–1
- DLQ ratio threshold
- schema consistency

Failed quality checks should be logged and surfaced in monitoring.

---

## 21. MLflow Design

Every training run should log:

### Parameters

- model type
- hyperparameters
- feature set
- train/test window
- class weights
- threshold

### Metrics

- PR-AUC
- ROC-AUC
- precision
- recall
- F1
- confusion matrix

### Artifacts

- model
- feature list
- plots
- dataset metadata

Model naming:

```text
sentinelpay-fraud-model
```

Versions:

```text
v1
v2
v3
```

---

## 22. Model Serving Strategy

### MVP

Load model artifact directly inside streaming job.

Advantages:
- Simple
- Low latency
- Easy local setup

### Future

Independent inference service.

Advantages:
- Model deployment independent from Spark
- Easier canary/champion-challenger

Trade-off:
- Network call increases latency and operational complexity

For this project, embedded inference is preferred initially.

---

## 23. Replay Design

Raw events must remain available.

Replay options:

### Kafka Replay

Reset consumer group offsets.

Useful for:
- stream logic changes
- bug reproduction

### S3 Replay

Read Bronze historical events and republish them.

Useful for:
- model backtests
- corrected pipeline versions

`scripts/replay.py` should support:

```text
--from
--to
--account-id
--rate
```

---

## 24. Observability

Metrics:

### Kafka

- producer rate
- consumer lag
- messages/sec
- failed messages

### Streaming

- batch duration
- input rows/sec
- processed rows/sec
- watermark delay
- state-store size

### Fraud

- approvals
- reviews
- blocks
- risk-score distribution
- alerts/min

### Data Quality

- validation failures
- duplicate rate
- DLQ rate

### ML

- prediction distribution
- precision
- recall
- drift metrics

---

## 25. Drift Monitoring

Three categories:

### Data Drift

Examples:

- amount distribution
- country distribution
- merchant-category distribution

### Prediction Drift

Changes in predicted fraud probabilities.

### Performance Drift

When delayed labels are available:

- precision over time
- recall over time
- PR-AUC over time

Possible statistical methods later:

- Population Stability Index
- Kolmogorov-Smirnov test

---

## 26. API Design

### GET `/health`

System health.

### GET `/alerts`

Recent fraud alerts.

Filters:

- decision
- min_risk
- account_id
- time range

### GET `/alerts/{transaction_id}`

Full fraud explanation.

### GET `/metrics/summary`

Operational summary.

### POST `/simulate/transaction`

Optional demo endpoint to inject one synthetic transaction.

---

## 27. Dashboard

Pages:

### Overview

- transactions/sec
- approvals/reviews/blocks
- fraud alert rate
- p95 latency
- Kafka lag

### Alerts

Table of fraud alerts.

### Alert Investigation

Show:

- transaction
- prior transactions
- model score
- rule triggers
- feature values
- geographic path

### Model

Show:

- precision
- recall
- PR curve
- confusion matrix
- model version

### Pipeline

Show:

- Kafka throughput
- DLQ count
- streaming batch durations
- data quality

---

## 28. Testing Strategy

### Unit Tests

- schema validation
- distance calculation
- rule triggers
- feature transformations
- risk aggregation

### Integration Tests

- producer → Kafka
- Kafka → Spark
- Spark → PostgreSQL
- Spark → S3
- alert generation

### Failure Tests

Test:

- malformed JSON
- missing fields
- duplicate transaction
- out-of-order event
- late event
- Kafka restart
- consumer restart
- database unavailable

### ML Tests

- no target leakage
- feature consistency
- model serialization
- threshold behavior

---

## 29. Benchmarking

`scripts/benchmark.py`

Measure at:

```text
10 events/sec
100 events/sec
500 events/sec
1000 events/sec
```

Track:

- throughput
- p50 decision latency
- p95 decision latency
- error rate
- Kafka lag
- Spark processing rate

Portfolio README should report actual measured results, not estimates.

---

## 30. Security and Privacy

Because this is a portfolio project:

- Use synthetic account IDs
- Never store real payment-card numbers
- Never store real PII
- Secrets kept in `.env` locally
- AWS credentials never committed
- IAM follows least-privilege principles
- Logs avoid sensitive payloads

---

## 31. Local Deployment

Docker Compose services:

```text
Kafka
Spark
PostgreSQL
MLflow
API
Producer
Dashboard
```

Optional Kafka UI may be included for development.

Goal:

```bash
docker compose up
```

should start most infrastructure.

---

## 32. Cloud Deployment

Recommended incremental deployment:

### Phase 1

Local Kafka + Spark  
AWS S3 + Glue + Athena

### Phase 2

Compute on EC2/ECS where practical.

Avoid expensive always-on managed streaming services unless needed.

---

## 33. CI/CD

GitHub Actions should eventually run:

- linting
- unit tests
- integration tests where practical
- model pipeline checks
- Docker image build

---

## 34. Design Trade-Offs

### Kafka vs Direct API Processing

Kafka chosen because:

- replay
- durability
- multiple consumers
- loose coupling
- partition scaling

### Spark vs Python Consumer

Spark chosen because:

- stateful streaming
- windowed aggregates
- watermark support
- scalable feature computation

### PostgreSQL vs Only S3

PostgreSQL stores operational low-latency alert state.

S3 stores analytical history.

### Rules + ML vs ML Only

Rules provide:

- explainability
- deterministic safety signals
- immediate detection for known patterns

ML captures more complex interactions.

---

## 35. Implementation Phases

## Phase 0 — Project Foundation

- repository
- PRD
- DESIGN
- environment
- Docker
- lint/test setup

## Phase 1 — Event Backbone

- synthetic generator
- Kafka
- topics
- schemas
- validation
- DLQ

## Phase 2 — Streaming Engine

- Spark
- event-time processing
- deduplication
- basic windows

## Phase 3 — Behavioral Features

- velocity
- amount
- geography
- device
- merchant

## Phase 4 — Risk Engine

- rules
- reason codes
- baseline risk scoring

## Phase 5 — ML

- dataset
- baseline
- XGBoost/LightGBM
- evaluation
- threshold analysis

## Phase 6 — Operational Persistence

- PostgreSQL
- API
- alerts

## Phase 7 — AWS Data Lake

- S3
- Bronze/Silver/Gold
- Glue
- Athena

## Phase 8 — MLOps

- MLflow
- model registry
- reproducibility

## Phase 9 — Data Quality + Observability

- Great Expectations
- metrics
- dashboard
- DLQ monitoring

## Phase 10 — Portfolio Hardening

- benchmark
- architecture diagram
- failure testing
- screenshots
- README
- deployment
