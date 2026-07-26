# PulseWatch

**A production-grade, fully local, real-time streaming analytics and anomaly detection platform.**

One command to start everything:

```bash
cp .env.example .env
docker compose up --build
```

---

## Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Service URLs](#service-urls)
- [Design Decisions](#design-decisions)
- [Anomaly Detection Methods](#anomaly-detection-methods)
- [Resource Tuning (Low-RAM Machines)](#resource-tuning)
- [Known Limitations](#known-limitations)
- [Development Notes](#development-notes)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Docker Compose Network                        │
│                                                                      │
│  ┌─────────────┐   raw-events   ┌──────────────────────────────────┐ │
│  │  Data Gen   │ ──────────────▶│  Apache Kafka (single broker)    │ │
│  │  (Python)   │                │  + Zookeeper                     │ │
│  └─────────────┘                └──────────┬───────────────────────┘ │
│                                            │ raw-events              │
│                                            ▼                         │
│                                 ┌──────────────────────┐             │
│                                 │  PySpark Structured  │             │
│                                 │  Streaming Job        │             │
│                                 │  • Z-score            │             │
│                                 │  • CUSUM              │             │
│                                 │  • Isolation Forest   │             │
│                                 └──────┬───────┬────────┘            │
│                                        │       │                     │
│                             events-raw │       │ events-anomalies    │
│                                        ▼       ▼                     │
│                                 ┌──────────────────────┐             │
│                                 │     OpenSearch        │             │
│                                 │  (ILM, daily rollover)│             │
│                                 └──────┬───────┬────────┘            │
│                                        │       │                     │
│                         ┌──────────────┘       └──────────┐          │
│                         ▼                                 ▼          │
│                  ┌─────────────┐                  ┌─────────────┐    │
│                  │   Grafana   │                  │   FastAPI   │    │
│                  │  (Ops dash) │                  │  REST + WS  │    │
│                  └─────────────┘                  └──────┬──────┘    │
│                                                          │           │
│                                             HTTP / WS    ▼           │
│                                                  ┌─────────────┐    │
│                                                  │   Next.js   │    │
│                                                  │  (frontend) │    │
│                                                  └─────────────┘    │
└──────────────────────────────────────────────────────────────────────┘

FastAPI runs an aiokafka consumer subscribed to anomaly-alerts → WebSocket push.
Spark Structured Streaming uses foreachBatch (10s trigger) for low-latency processing.
```

### Data Flow

1. **Data Generator** (`data-generator/`) — Simulates 5 IoT sensors publishing JSON events every 500ms to Kafka topic `raw-events`, partitioned by `sensor_id` to preserve per-sensor ordering.

2. **PySpark Streaming Job** (`spark-job/`) — Consumes `raw-events`, applies Z-score, CUSUM, and periodic Isolation Forest scoring per event. Writes enriched events to `events-raw` and anomaly records to `events-anomalies` in OpenSearch. Publishes anomalies to Kafka topic `anomaly-alerts`.

3. **FastAPI Backend** (`backend/`) — REST API over OpenSearch for historical queries. A background `aiokafka` consumer subscribes to `anomaly-alerts` and pushes records over WebSocket to connected frontend clients.

4. **Next.js Frontend** (`frontend/`) — Minimalist single-page dashboard: time series chart with rolling mean band and anomaly markers, live alert feed, entity selector, system status strip.

5. **Grafana** — Separate operational dashboard for debugging/verification, connected directly to OpenSearch.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Docker Desktop | ≥ 4.x with WSL2 backend (Windows) |
| Available RAM | **Minimum 8 GB** assigned to Docker Desktop |
| Recommended RAM | **16 GB** for smooth operation |
| Disk space | ~5 GB (images + data) |

### ⚠️ Windows / WSL2 — Required One-Time Setup

OpenSearch requires a Linux kernel parameter that must be set in WSL2:

```powershell
# Run once in PowerShell (admin not required)
wsl -d docker-desktop sysctl -w vm.max_map_count=262144
```

This resets on every Docker Desktop restart. To make it permanent, add to `/etc/sysctl.conf` inside WSL2:

```bash
# Inside WSL2:
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf
```

---

## Quick Start

```bash
# 1. Clone and enter the project
cd pulseWatch

# 2. Copy environment config (edit if needed)
cp .env.example .env

# 3. (Windows only — one-time) Set vm.max_map_count
wsl -d docker-desktop sysctl -w vm.max_map_count=262144

# 4. Start everything
docker compose up --build

# Wait ~2-3 minutes for all services to reach healthy state.
# Watch progress with:
docker compose ps
```

To rebuild a specific service after code changes:

```bash
docker compose up --build backend
```

To stop everything and remove volumes (full reset):

```bash
docker compose down -v
```

---

## Service URLs

| Service | URL | Notes |
|---|---|---|
| **Frontend** | http://localhost:3001 | Main product dashboard |
| **Grafana** | http://localhost:3000 | Ops dashboard (admin / admin) |
| **FastAPI** | http://localhost:8000 | REST API |
| **API Docs** | http://localhost:8000/docs | Interactive Swagger UI |
| **Health check** | http://localhost:8000/health | JSON health status |
| **WebSocket** | ws://localhost:8000/ws/anomalies | Real-time anomaly stream |
| **OpenSearch** | http://localhost:9200 | Direct cluster access |
| **Kafka** | localhost:29092 | External listener (host access) |

---

## Design Decisions

### Why Kafka instead of REST polling?

Kafka provides durable, ordered, partitioned log storage for events. Producers and consumers are fully decoupled — the generator can continue publishing even if Spark is temporarily down, and Spark will catch up when it recovers. This is impossible with REST polling. The tradeoff is operational complexity (Zookeeper, partition management) which is why this setup is appropriate for a demo platform but would use a managed Kafka service (Confluent Cloud, MSK) in production.

### Why OpenSearch instead of Elasticsearch?

Elasticsearch moved to the Server Side Public License (SSPL) in 2021, which is not OSI-approved open source. OpenSearch (Apache 2.0) is the community fork backed by Amazon, supports the same REST API and query DSL, and has no licensing restrictions for self-hosting or incorporating into products. Feature parity for our use case (ILM, aggregations, full-text search) is complete.

### Why Z-score + CUSUM + Isolation Forest instead of just one method?

Each method catches a different class of anomaly:

| Method | What it detects | Limitation |
|---|---|---|
| **Z-score** | Sudden, large spikes (|z| > 3σ) | Misses slow drift; requires stable baseline |
| **CUSUM** | Gradual cumulative drift away from target | Misses sudden spikes; sensitive to target choice |
| **Isolation Forest** | Multivariate outliers (unusual combinations of all three metrics) | Needs enough training data; batch-trained, not online |

Using all three provides complementary coverage. A sensor might show normal temperature but anomalous vibration+pressure together — only IF catches that. A sensor that slowly drifts 2σ over 30 minutes won't trigger z-score but will trigger CUSUM.

### Why `aiokafka` for WebSocket push instead of OpenSearch polling?

OpenSearch polling would introduce at minimum 1–5 seconds of latency (round-trip query interval). The `aiokafka` async consumer receives messages from Kafka within milliseconds of the Spark job publishing them, enabling genuine sub-second anomaly alerts to the frontend.

### Why `bitnami/spark` base image?

The official `apache/spark` Docker image does not ship Python by default and requires significant setup for PySpark. For a local dev platform on Windows, `bitnami/spark` provides a fully configured Python + Spark environment that works reliably across platforms.

### Why Recharts for the time series chart?

Recharts is MIT-licensed, React-native, requires no server-side rendering configuration, and produces clean, composable SVG charts. The composable API (ComposedChart + Line + Scatter) lets us layer the raw reading, rolling mean band, and anomaly scatter points in a single chart without workarounds.

---

## Anomaly Detection Methods

### Z-score Detection

For each metric `m` in `{temperature, vibration, pressure}`:

```
z = (value - μ) / σ
flag if |z| > threshold (default: 3.0)
```

where `μ` and `σ` are the known normal operating parameters per sensor type. The threshold is configurable via `ZSCORE_THRESHOLD` in `.env`.

### CUSUM (Cumulative Sum Control Chart)

```
S_pos[t] = max(0, S_pos[t-1] + (x[t] - μ - k))
S_neg[t] = max(0, S_neg[t-1] + (μ - x[t] - k))
flag if S_pos > h OR S_neg > h
```

where `k` = allowable slack (`CUSUM_SLACK`) and `h` = detection threshold (`CUSUM_THRESHOLD`). CUSUM accumulates evidence of sustained drift — a single outlier won't trigger it, but sustained deviation will.

### Isolation Forest

A scikit-learn `IsolationForest` is trained every N minutes (`IF_RETRAIN_INTERVAL_MIN`) on the rolling history of the last 1000 events per sensor. It uses all three metrics as features and flags observations with anomaly score < -0.1 (contamination=0.05). Being multivariate, it can detect unusual *combinations* that no single-metric method would catch.

---

## Resource Tuning

If you have limited RAM (< 8 GB available to Docker), reduce resource usage with these `.env` changes:

```bash
# Reduce Spark memory (from 1g each)
SPARK_EXECUTOR_MEMORY=512m
SPARK_DRIVER_MEMORY=512m

# Reduce OpenSearch heap (edit docker-compose.yml)
# OPENSEARCH_JAVA_OPTS: "-Xms256m -Xmx256m"

# Use fewer Kafka partitions (reduces memory per partition)
KAFKA_PARTITIONS=1

# Retrain Isolation Forest less frequently
IF_RETRAIN_INTERVAL_MIN=15
```

You can also disable Grafana if you don't need the ops dashboard:

```bash
docker compose up zookeeper kafka kafka-init opensearch opensearch-init \
                  data-generator spark-job backend frontend
```

---

## Known Limitations

| Limitation | Impact | Production solution |
|---|---|---|
| **Single-broker Kafka** | No fault tolerance — broker failure loses unread messages | Multi-broker Kafka cluster (min 3 brokers) |
| **No TLS/auth on Kafka** | Any process on the Docker network can produce/consume | Enable SASL + TLS, use Kafka ACLs |
| **OpenSearch security disabled** | No auth, no encryption | Enable OpenSearch security plugin, configure TLS certificates |
| **No Kafka auth** | Open broker | Configure SASL/PLAIN or SASL/SCRAM |
| **CUSUM state in driver memory** | State lost on Spark job restart | Use Spark structured streaming stateful ops with checkpointing |
| **IF training is synchronous** | Blocks micro-batch for ~1–2s every N minutes | Move to a separate training service or use Spark ML pipelines |
| **Single OpenSearch node** | No replica shards, data loss on node failure | Multi-node OpenSearch cluster |
| **No authentication on APIs** | Anyone on the network can call the API | Add OAuth2 / API key auth to FastAPI |
| **vm.max_map_count** | Must be set manually on Windows WSL2 restart | Use a startup script or Makefile |

This platform is designed as a **portfolio/development demonstration**. It is not suitable for production traffic without addressing the above.

---

## Development Notes

### Verify Stage-by-Stage

```bash
# Stage 1 — Kafka topics created
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Stage 2 — Generator producing events
docker compose logs data-generator | head -40

# Stage 3/4 — Spark writing to OpenSearch
curl http://localhost:9200/events-raw/_count

# Stage 5 — Anomalies being detected
curl http://localhost:9200/events-anomalies/_count

# Stage 7 — Backend health
curl http://localhost:8000/health | python -m json.tool

# Stage 7 — Query anomalies
curl "http://localhost:8000/anomalies?limit=5" | python -m json.tool

# Stage 8 — Frontend
open http://localhost:3001
```

### Environment Variables Reference

See [`.env.example`](.env.example) for all available configuration options with descriptions.

### Logs

```bash
docker compose logs -f backend       # FastAPI structured JSON logs
docker compose logs -f spark-job     # Spark streaming job
docker compose logs -f data-generator # Event generator
```
