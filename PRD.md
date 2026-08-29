# PRD — Real-Time Payment Risk & Fraud Intelligence Platform

**Project codename:** SentinelPay  
**Document type:** Product Requirements Document  
**Version:** v1.0  
**Primary goal:** Build a production-style real-time payment risk platform that demonstrates strong Data Engineering, Data Science, and ML Engineering fundamentals.

---

## 1. Executive Summary

SentinelPay is a real-time payment risk and fraud intelligence platform that ingests transaction events, validates and enriches them, computes streaming behavioral features, scores each transaction using rules and machine-learning models, stores analytical data in a medallion-style data lake, and exposes fraud alerts and operational metrics through APIs and dashboards.

The project is intentionally designed to go beyond a typical “fraud prediction notebook.” The core challenge is not only training a classifier, but designing a reliable streaming system that handles event ordering, late events, duplicates, schema evolution, retries, dead-letter events, replay, model versioning, and data quality.

The final system should demonstrate the ability to reason about:

- Event-driven architecture
- Kafka partitioning and consumer groups
- Spark Structured Streaming
- Event-time processing and watermarks
- Idempotency and deduplication
- Feature engineering for fraud
- Imbalanced classification
- ML model versioning and experiment tracking
- Cloud data lake design
- Data quality monitoring
- Production observability

---

## 2. Problem Statement

Payment systems must evaluate transactions in near real time while balancing two competing objectives:

1. Detect as much fraud as possible.
2. Avoid blocking legitimate customers.

Traditional batch-only systems are insufficient for real-time fraud because many of the most predictive features depend on recent behavior, such as:

- Number of transactions in the last 5 minutes
- Sudden amount spikes
- Impossible travel between locations
- Previously unseen devices
- Rapid merchant changes
- High-risk merchant categories
- Unusual time-of-day behavior

A robust fraud platform must process events continuously, maintain historical state, make low-latency decisions, and preserve enough information to support investigation, analytics, replay, and model retraining.

---

## 3. Product Vision

Create a realistic fraud intelligence platform that could conceptually sit between a payment gateway and downstream banking systems.

For every transaction, the platform should produce:

- A normalized transaction record
- Data-quality status
- Streaming behavioral features
- Rules-based risk signals
- ML fraud probability
- Final risk score
- Decision: APPROVE / REVIEW / BLOCK
- Human-readable reason codes
- Persisted audit trail
- Operational metrics

Example output:

```json
{
  "transaction_id": "txn_82391",
  "account_id": "acct_1008",
  "risk_score": 0.93,
  "decision": "BLOCK",
  "model_version": "fraud-xgb-v3",
  "reason_codes": [
    "IMPOSSIBLE_TRAVEL",
    "TXN_VELOCITY_5M",
    "AMOUNT_DEVIATION",
    "NEW_DEVICE"
  ]
}
```

---

## 4. Target Users

### Primary

**Fraud Analyst**
- Reviews flagged transactions
- Investigates risk signals
- Understands why a transaction was blocked

**Data Engineer**
- Operates ingestion and streaming pipelines
- Handles schema changes, late events, retries, and replay
- Ensures reliability and data quality

**Data Scientist / ML Engineer**
- Builds and evaluates fraud models
- Monitors model quality and drift
- Promotes model versions

### Secondary

**Platform / Operations Engineer**
- Monitors throughput, errors, consumer lag, and latency

---

## 5. Project Goals

### G1 — Real-Time Event Processing
Ingest and process payment events continuously using Kafka and Spark Structured Streaming.

### G2 — Behavioral Feature Engineering
Create streaming and historical features that reflect user, device, merchant, and geographic behavior.

### G3 — Risk Scoring
Combine deterministic rules and ML predictions into an explainable risk decision.

### G4 — Reliable Data Platform
Handle malformed data, duplicates, out-of-order events, retries, schema changes, and replay.

### G5 — Cloud Analytics
Persist Bronze, Silver, and Gold datasets in AWS S3 and make them queryable through Glue/Athena.

### G6 — ML Lifecycle
Track experiments and models using MLflow and support model versioning.

### G7 — Observability
Expose metrics for system health, data quality, fraud decisions, and model performance.

---

## 6. Non-Goals

The first version will not attempt to:

- Integrate with a real bank or payment processor
- Process personally identifiable information
- Provide PCI-DSS-certified infrastructure
- Perform actual money movement
- Support production-scale billions of daily transactions
- Build a full enterprise case-management application
- Use personally identifiable customer data
- Implement graph neural networks in v1
- Implement deep learning unless justified by later experiments

---

## 7. Core User Stories

### Fraud Analyst

**US-1**  
As a fraud analyst, I want to see high-risk transactions so I can investigate suspicious activity.

**US-2**  
As a fraud analyst, I want reason codes for each decision so I can understand why a transaction was flagged.

**US-3**  
As a fraud analyst, I want historical account and transaction context so I can validate suspicious patterns.

### Data Engineer

**US-4**  
As a data engineer, I want malformed messages routed to a dead-letter topic so the pipeline does not fail.

**US-5**  
As a data engineer, I want duplicate transactions handled idempotently so retries do not create incorrect records.

**US-6**  
As a data engineer, I want late events handled using event-time semantics so time-windowed features remain accurate.

**US-7**  
As a data engineer, I want raw events preserved so historical transactions can be replayed.

### Data Scientist

**US-8**  
As a data scientist, I want labeled historical data and engineered features so I can train fraud models.

**US-9**  
As a data scientist, I want experiments, metrics, and model versions tracked.

**US-10**  
As a data scientist, I want fraud thresholds configurable so precision/recall trade-offs can be evaluated.

---

## 8. Functional Requirements

## FR-1 Transaction Ingestion

The system must ingest synthetic or public fraud transaction data through Kafka.

Required fields:

- transaction_id
- account_id
- event_timestamp
- amount
- currency
- merchant_id
- merchant_category
- device_id
- latitude
- longitude
- country
- channel
- transaction_type

Optional fields:

- ip_address_hash
- card_present
- authentication_method
- merchant_country

---

## FR-2 Schema Validation

Each event must be validated before downstream processing.

Validation includes:

- Required-field checks
- Correct data types
- Positive transaction amount
- Valid timestamps
- Supported currencies
- Valid coordinates
- Unique transaction identifier
- Valid enum values

Invalid events must be routed to:

`transactions.dlq`

---

## FR-3 Kafka Topics

Minimum topics:

- `transactions.raw`
- `transactions.validated`
- `fraud.alerts`
- `transactions.dlq`

Optional:

- `features.updated`
- `transactions.enriched`

---

## FR-4 Streaming Feature Engineering

The platform must compute rolling behavioral features.

### Velocity Features

- txn_count_5m
- txn_count_30m
- txn_count_24h
- amount_sum_5m
- amount_sum_30m
- amount_sum_24h

### Amount Features

- account_mean_amount_30d
- account_std_amount_30d
- amount_zscore
- amount_percentile
- amount_vs_recent_average

### Geographic Features

- distance_from_previous_txn
- time_since_previous_txn
- implied_travel_speed
- country_changed
- impossible_travel

### Device Features

- new_device_for_account
- device_account_count
- device_txn_count_1h
- device_country_change

### Merchant Features

- merchant_fraud_rate
- new_merchant_for_account
- merchant_category_risk
- merchant_txn_velocity

### Temporal Features

- hour_of_day
- day_of_week
- unusual_hour_for_account
- seconds_since_previous_txn

---

## FR-5 Rules Engine

The system must support deterministic risk rules.

Example rules:

- Impossible geographic travel
- Excessive transaction velocity
- Extreme transaction amount deviation
- New device + high transaction amount
- High-risk merchant category
- Rapid multi-country activity

Each triggered rule must generate a reason code.

---

## FR-6 ML Fraud Scoring

The system must support at least:

- Logistic Regression baseline
- XGBoost or LightGBM
- Isolation Forest as an unsupervised comparison

Primary model evaluation metrics:

- Precision
- Recall
- F1
- PR-AUC
- ROC-AUC
- False-positive rate
- False-negative rate

Accuracy must not be used as the primary metric.

---

## FR-7 Risk Decision

The platform must combine ML probability and rule-based signals into a final score.

Example decision policy:

- 0.00–0.49 → APPROVE
- 0.50–0.79 → REVIEW
- 0.80–1.00 → BLOCK

Thresholds must be configurable.

---

## FR-8 Data Lake

Persist transaction data to AWS S3 using a medallion architecture.

### Bronze
Immutable raw events.

### Silver
Validated, deduplicated, normalized, enriched transactions.

### Gold
Analytics-ready data such as:

- fraud features
- risk scores
- daily merchant summaries
- account-risk summaries
- model monitoring aggregates

---

## FR-9 Analytical Querying

AWS Glue Data Catalog and Athena should support ad hoc queries over Silver and Gold datasets.

Examples:

- Fraud rate by merchant category
- Fraud rate by country
- Average risk score by channel
- Daily transaction count and value
- Model false-positive rate over time

---

## FR-10 Operational Database

PostgreSQL should store operational entities such as:

- current fraud alerts
- transaction risk decisions
- model decision metadata
- analyst review status

---

## FR-11 ML Experiment Tracking

MLflow must track:

- training dataset version
- model parameters
- feature set
- metrics
- model artifact
- decision threshold
- model version

---

## FR-12 Dashboard / API

The project should expose:

### Dashboard metrics

- Total transactions
- Transactions per second
- Fraud alerts
- Fraud alert rate
- Average risk score
- p50 / p95 scoring latency
- Consumer lag
- DLQ count
- Data-quality failure rate

### Fraud alert details

- Transaction ID
- Account ID
- Risk score
- Decision
- Model probability
- Triggered rules
- Reason codes
- Timestamp

---

## 9. Non-Functional Requirements

### Performance

Target for local/demo environment:

- Sustain at least 100 events/second
- Target 500+ events/second stretch goal
- p95 streaming risk-decision latency < 3 seconds
- Dashboard queries < 2 seconds for operational views

### Reliability

The system should:

- Avoid pipeline termination on malformed events
- Support replay from Kafka/S3
- Deduplicate transactions
- Recover after consumer restart
- Persist raw source events

### Scalability

Partition Kafka topics by `account_id` to preserve per-account ordering while allowing horizontal scaling.

### Maintainability

Services must be independently testable and containerized.

### Explainability

Every REVIEW or BLOCK decision must include at least one reason code.

---

## 10. Data Requirements

Preferred options:

1. Public credit-card fraud dataset for model training
2. Synthetic transaction generator for streaming simulation
3. Hybrid setup:
   - Train with public historical data
   - Generate realistic live events for Kafka

Synthetic generator should support:

- configurable transaction rate
- fraud injection probability
- geographic anomalies
- device anomalies
- velocity attacks
- amount spikes

---

## 11. Success Metrics

The project is successful when:

### Engineering

- Kafka processes 100+ events/sec reliably
- Duplicate events do not create duplicate transactions
- Invalid events reach DLQ
- Late events are handled through event-time windows
- Bronze/Silver/Gold datasets are created successfully
- Kafka replay works
- Services restart without data corruption

### ML

Targets depend on dataset, but the project should demonstrate:

- Strong PR-AUC vs baseline
- Meaningful improvement over Logistic Regression baseline
- Explicit precision/recall threshold trade-off
- Model versioning in MLflow

### Portfolio Quality

GitHub repository includes:

- Architecture diagram
- Local quick-start
- Data model
- Kafka design
- Feature definitions
- Model evaluation
- Benchmark results
- Failure-mode documentation
- Screenshots
- Cloud deployment notes

---

## 12. MVP Scope

The MVP should include:

1. Synthetic transaction producer
2. Kafka broker
3. Schema validation
4. Spark streaming consumer
5. 8–12 behavioral features
6. Rule engine
7. One baseline ML model
8. Fraud decision API/output
9. PostgreSQL alert storage
10. Docker Compose local environment
11. Basic metrics/dashboard
12. Basic automated tests

AWS should be added immediately after the local MVP is stable.

---

## 13. V1 Scope

After MVP:

- AWS S3 Bronze/Silver/Gold
- Glue + Athena
- MLflow
- Great Expectations
- More advanced streaming features
- Event-time windows and watermarks
- DLQ tooling
- Model comparison
- Replay workflow
- Monitoring dashboard

---

## 14. Stretch Goals

- Redis/Feast online feature store
- Schema Registry + Avro
- Kafka Connect
- Debezium integration
- Graph-based fraud signals
- Champion/challenger models
- Drift detection
- Prometheus + Grafana
- ECS deployment
- Terraform
- CI/CD
- Real-time alert notifications

---

## 15. Key Risks

### R1 — Overengineering
The stack could become too large before a working pipeline exists.

**Mitigation:** Build local vertical slice first.

### R2 — Dataset mismatch
Historical datasets may lack fields needed for realistic streaming features.

**Mitigation:** Use synthetic enrichment and clearly document assumptions.

### R3 — Fraud class imbalance
Accuracy can appear strong while fraud recall is poor.

**Mitigation:** Use PR-AUC, precision, recall, threshold analysis, class weighting.

### R4 — Cloud cost
Running Kafka/Spark/Databricks continuously can be expensive.

**Mitigation:** Use Docker locally; limit AWS to low-cost services and short-lived compute.

### R5 — Tutorial-like implementation
A project using many technologies without architectural reasoning may appear superficial.

**Mitigation:** Document every major design choice and trade-off.

---

## 16. Definition of Done

The project is considered portfolio-ready when:

- End-to-end transaction flow works
- Fraud decisions are produced in near real time
- Features are explainable
- Model evaluation is rigorous
- Kafka design decisions are documented
- DLQ, replay, deduplication, and late events are demonstrated
- AWS lakehouse path works
- System is benchmarked
- README clearly communicates architecture, trade-offs, and results
