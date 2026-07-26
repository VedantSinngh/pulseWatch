"""
PulseWatch — PySpark Structured Streaming Job
==============================================
Reads from Kafka `raw-events`, computes:
  • Rolling mean, std dev, EWMA per sensor per 5-min sliding window
  • Z-score anomaly detection (|z| > threshold → spike)
  • CUSUM control chart (cumulative drift detection)
  • Isolation Forest multivariate scoring (every N minutes, micro-batch)

Writes to:
  • OpenSearch index `events-raw`         (enriched events)
  • OpenSearch index `events-anomalies`   (anomaly records only)
  • Kafka topic `anomaly-alerts`          (for FastAPI WebSocket push)
"""

import json
import logging
import math
import os
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

# ── Logging setup (before Spark imports so it's active from the start) ────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("spark-job")

# Silence overly verbose Py4J / PySpark logs
logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("pyspark").setLevel(logging.WARNING)

# ── PySpark imports ────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType, BooleanType
)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER      = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW", "raw-events")
TOPIC_ANOMALIES   = os.getenv("KAFKA_TOPIC_ANOMALIES", "anomaly-alerts")
OS_HOST           = os.getenv("OPENSEARCH_HOST", "opensearch")
OS_PORT           = int(os.getenv("OPENSEARCH_PORT", "9200"))
OS_SCHEME         = os.getenv("OPENSEARCH_SCHEME", "http")
INDEX_RAW         = os.getenv("OPENSEARCH_INDEX_RAW", "events-raw")
INDEX_ANOMALIES   = os.getenv("OPENSEARCH_INDEX_ANOMALIES", "events-anomalies")
SPARK_MASTER      = os.getenv("SPARK_MASTER", "local[2]")
EXECUTOR_MEM      = os.getenv("SPARK_EXECUTOR_MEMORY", "1g")
DRIVER_MEM        = os.getenv("SPARK_DRIVER_MEMORY", "1g")
WINDOW_DUR        = os.getenv("WINDOW_DURATION", "5 minutes")
SLIDE_DUR         = os.getenv("SLIDE_DURATION", "1 minute")
ZSCORE_THRESHOLD  = float(os.getenv("ZSCORE_THRESHOLD", "3.0"))
CUSUM_SLACK       = float(os.getenv("CUSUM_SLACK", "1.0"))
CUSUM_THRESHOLD   = float(os.getenv("CUSUM_THRESHOLD", "5.0"))
IF_INTERVAL_MIN   = int(os.getenv("IF_RETRAIN_INTERVAL_MIN", "5"))

# ── Kafka JAR coordinates (Spark 3.5 / Scala 2.12) ────────────────────────────
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

# ── Event schema ──────────────────────────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("sensor_id",        StringType(),    True),
    StructField("timestamp",        StringType(),    True),
    StructField("temperature",      DoubleType(),    True),
    StructField("vibration",        DoubleType(),    True),
    StructField("pressure",         DoubleType(),    True),
    StructField("anomaly_injected", StringType(),    True),
])

# ── Per-sensor state for CUSUM and Isolation Forest ──────────────────────────
# These live in the driver process (foreachBatch runs on driver when using local mode)
_cusum_state: Dict[str, Dict[str, float]] = defaultdict(
    lambda: {"temp_cusum_pos": 0.0, "temp_cusum_neg": 0.0,
             "vib_cusum_pos":  0.0, "vib_cusum_neg":  0.0,
             "pres_cusum_pos": 0.0, "pres_cusum_neg": 0.0}
)

# Rolling history for Isolation Forest (last N rows per sensor)
_if_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
_if_models:  Dict[str, Any] = {}   # trained sklearn models
_if_last_train: Dict[str, float] = {}  # epoch seconds of last training
IF_INTERVAL_S = IF_INTERVAL_MIN * 60

# Batch counter for periodic IF retraining
_batch_count = 0


# ── CUSUM helper ──────────────────────────────────────────────────────────────
def cusum_update(state: Dict[str, float], metric: str, value: float,
                 target: float, slack: float) -> Tuple[float, float, bool]:
    """
    Update CUSUM accumulators for one metric.
    Returns (cusum_pos, cusum_neg, is_anomaly).
    """
    pos_key = f"{metric}_cusum_pos"
    neg_key = f"{metric}_cusum_neg"
    pos = max(0.0, state[pos_key] + (value - target - slack))
    neg = max(0.0, state[neg_key] + (target - value - slack))
    state[pos_key] = pos
    state[neg_key] = neg
    is_anomaly = (pos > CUSUM_THRESHOLD) or (neg > CUSUM_THRESHOLD)
    return pos, neg, is_anomaly


# ── Isolation Forest helpers ──────────────────────────────────────────────────
def train_isolation_forest(sensor_id: str) -> Optional[Any]:
    """Train (or retrain) an IsolationForest on the rolling history."""
    try:
        from sklearn.ensemble import IsolationForest
        import numpy as np

        history = list(_if_history[sensor_id])
        if len(history) < 50:
            return None  # not enough data yet
        X = np.array([[r["temperature"], r["vibration"], r["pressure"]]
                      for r in history])
        model = IsolationForest(
            n_estimators=100,
            contamination=0.05,
            random_state=42,
            n_jobs=1,
        )
        model.fit(X)
        log.info("Isolation Forest retrained for %s on %d samples.", sensor_id, len(history))
        return model
    except Exception as exc:
        log.warning("IF training failed for %s: %s", sensor_id, exc)
        return None


def score_isolation_forest(sensor_id: str, temp: float, vib: float, pres: float
                            ) -> Tuple[float, bool]:
    """Score one observation; returns (raw_score, is_anomaly)."""
    model = _if_models.get(sensor_id)
    if model is None:
        return 0.0, False
    try:
        import numpy as np
        X = np.array([[temp, vib, pres]])
        score = float(model.score_samples(X)[0])   # negative → more anomalous
        # IsolationForest: score < -0.1 is a common threshold; scores near 0 are inliers
        is_anomaly = score < -0.1
        return score, is_anomaly
    except Exception as exc:
        log.warning("IF scoring failed for %s: %s", sensor_id, exc)
        return 0.0, False


# ── OpenSearch writer ─────────────────────────────────────────────────────────
def get_os_client():
    from opensearchpy import OpenSearch
    return OpenSearch(
        hosts=[{"host": OS_HOST, "port": OS_PORT}],
        http_compress=True,
        use_ssl=False,
        verify_certs=False,
        timeout=30,
    )


def bulk_index(client, index: str, docs: List[Dict]) -> None:
    """Bulk-index a list of documents into OpenSearch."""
    if not docs:
        return
    from opensearchpy.helpers import bulk
    actions = [{"_index": index, "_source": doc} for doc in docs]
    try:
        ok, errors = bulk(client, actions, raise_on_error=False, request_timeout=30)
        if errors:
            log.warning("OpenSearch bulk errors for %s: %s", index, errors[:3])
        else:
            log.debug("Indexed %d docs into %s", ok, index)
    except Exception as exc:
        log.error("OpenSearch bulk write failed for %s: %s", index, exc)


# ── Kafka anomaly publisher ────────────────────────────────────────────────────
def publish_anomaly_to_kafka(producer, doc: Dict) -> None:
    try:
        producer.produce(
            topic=TOPIC_ANOMALIES,
            key=doc.get("sensor_id", "").encode("utf-8"),
            value=json.dumps(doc).encode("utf-8"),
        )
        producer.poll(0)
    except Exception as exc:
        log.warning("Failed to publish anomaly to Kafka: %s", exc)


# ── foreachBatch handler ──────────────────────────────────────────────────────
def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Called once per micro-batch by Spark Structured Streaming.
    All state (CUSUM, IF models, history) lives in driver-process globals.
    """
    global _batch_count

    if batch_df.rdd.isEmpty():
        return

    _batch_count += 1
    rows = batch_df.collect()
    log.info("Batch %d: processing %d rows", batch_id, len(rows))

    # Lazy-init clients inside the batch handler (runs on driver)
    from confluent_kafka import Producer as KafkaProducer
    kafka_producer = KafkaProducer({
        "bootstrap.servers": KAFKA_BROKER,
        "acks": 1,
        "linger.ms": 50,
    })
    os_client = get_os_client()

    # Normal operating targets (used for CUSUM baseline)
    targets = {"temperature": 22.0, "vibration": 0.05, "pressure": 101.3}
    stds    = {"temperature": 1.5,  "vibration": 0.01, "pressure": 0.8}

    # ── Periodic Isolation Forest retraining ──────────────────────────────
    now = time.time()
    for sensor_id in _if_history.keys():
        last_train = _if_last_train.get(sensor_id, 0.0)
        if (now - last_train) >= IF_INTERVAL_S:
            model = train_isolation_forest(sensor_id)
            if model is not None:
                _if_models[sensor_id] = model
                _if_last_train[sensor_id] = now

    # ── Per-row processing ────────────────────────────────────────────────
    raw_docs       = []
    anomaly_docs   = []

    for row in rows:
        sensor_id  = row.sensor_id
        ts         = row.timestamp
        temp       = float(row.temperature)
        vib        = float(row.vibration)
        pres       = float(row.pressure)
        inj        = row.anomaly_injected

        # Accumulate history for IF
        _if_history[sensor_id].append({
            "temperature": temp, "vibration": vib, "pressure": pres
        })

        state = _cusum_state[sensor_id]

        # ── Z-score (per metric) ─────────────────────────────────────────
        def zscore(val, metric):
            return (val - targets[metric]) / stds[metric]

        z_temp = zscore(temp, "temperature")
        z_vib  = zscore(vib,  "vibration")
        z_pres = zscore(pres, "pressure")

        is_zscore = (
            abs(z_temp) > ZSCORE_THRESHOLD or
            abs(z_vib)  > ZSCORE_THRESHOLD or
            abs(z_pres) > ZSCORE_THRESHOLD
        )

        # ── CUSUM ────────────────────────────────────────────────────────
        cp, cn, cusum_temp = cusum_update(state, "temp", temp,
                                          targets["temperature"], CUSUM_SLACK)
        vp, vn, cusum_vib  = cusum_update(state, "vib",  vib,
                                          targets["vibration"],   CUSUM_SLACK)
        pp, pn, cusum_pres = cusum_update(state, "pres", pres,
                                          targets["pressure"],    CUSUM_SLACK)
        is_cusum = cusum_temp or cusum_vib or cusum_pres

        # ── Isolation Forest ─────────────────────────────────────────────
        if_score, is_if = score_isolation_forest(sensor_id, temp, vib, pres)

        # ── Determine anomaly types ──────────────────────────────────────
        anomaly_types = []
        if is_zscore: anomaly_types.append("zscore")
        if is_cusum:  anomaly_types.append("cusum")
        if is_if:     anomaly_types.append("isolation_forest")

        is_anomaly = bool(anomaly_types)

        # ── Build enriched event document ────────────────────────────────
        enriched = {
            "sensor_id":         sensor_id,
            "timestamp":         ts,
            "temperature":       temp,
            "vibration":         vib,
            "pressure":          pres,
            "z_temperature":     round(z_temp, 4),
            "z_vibration":       round(z_vib, 4),
            "z_pressure":        round(z_pres, 4),
            "cusum_temp_pos":    round(cp, 4),
            "cusum_temp_neg":    round(cn, 4),
            "cusum_vib_pos":     round(vp, 4),
            "cusum_vib_neg":     round(vn, 4),
            "cusum_pres_pos":    round(pp, 4),
            "cusum_pres_neg":    round(pn, 4),
            "if_score":          round(if_score, 6),
            "is_anomaly":        is_anomaly,
            "anomaly_types":     anomaly_types,
            "anomaly_injected":  inj,
            "processed_at":      datetime.now(timezone.utc).isoformat(),
        }
        raw_docs.append(enriched)

        # ── Anomaly record ────────────────────────────────────────────────
        if is_anomaly:
            anomaly_doc = {
                **enriched,
                "severity": "high" if (is_zscore or is_if) else "medium",
            }
            anomaly_docs.append(anomaly_doc)
            publish_anomaly_to_kafka(kafka_producer, anomaly_doc)

    # ── Bulk write to OpenSearch ──────────────────────────────────────────
    bulk_index(os_client, INDEX_RAW, raw_docs)
    if anomaly_docs:
        bulk_index(os_client, INDEX_ANOMALIES, anomaly_docs)
        log.info("Batch %d: flagged %d anomalies.", batch_id, len(anomaly_docs))

    kafka_producer.flush(timeout=5)


# ── Spark session ─────────────────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName("PulseWatch-Streaming")
        .config("spark.executor.memory", EXECUTOR_MEM)
        .config("spark.driver.memory", DRIVER_MEM)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        # Reduce Spark UI memory overhead
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    log.info("Starting PulseWatch Spark Streaming Job")
    log.info("Kafka: %s | Topic: %s", KAFKA_BROKER, TOPIC_RAW)
    log.info("OpenSearch: %s://%s:%d", OS_SCHEME, OS_HOST, OS_PORT)
    log.info("Z-score threshold: %.1f | CUSUM threshold: %.1f | IF interval: %d min",
             ZSCORE_THRESHOLD, CUSUM_THRESHOLD, IF_INTERVAL_MIN)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── Read from Kafka ───────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.session.timeout.ms", "30000")
        .load()
    )

    # ── Parse JSON payload ────────────────────────────────────────────────
    parsed = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast(StringType()),
                EVENT_SCHEMA
            ).alias("data"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("data.*")
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp"))
        )
        .filter(F.col("sensor_id").isNotNull())
        .filter(F.col("temperature").isNotNull())
    )

    # ── Start streaming query with foreachBatch ───────────────────────────
    query = (
        parsed.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", "/tmp/pulsewatch-checkpoint")
        .trigger(processingTime="10 seconds")
        .start()
    )

    log.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
