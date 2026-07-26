"""
PulseWatch — IoT Sensor Event Generator
Simulates 5 sensors (temperature, vibration, pressure) publishing to Kafka.
Injects realistic anomalies: sudden spikes and slow gradual drifts.
"""

import json
import logging
import math
import os
import random
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("generator")

# ── Config ───────────────────────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "raw-events")
PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "3"))

SENSORS = [f"sensor-{i:03d}" for i in range(1, 6)]
PUBLISH_INTERVAL_S = 0.5   # publish every 500 ms per sensor

# Normal operating ranges (mean, std)
SENSOR_PROFILE = {
    "temperature": {"mean": 22.0, "std": 1.5, "unit": "°C"},
    "vibration":   {"mean": 0.05, "std": 0.01, "unit": "g"},
    "pressure":    {"mean": 101.3, "std": 0.8, "unit": "kPa"},
}

# Anomaly injection probabilities (per publish cycle, per sensor)
SPIKE_PROB = 0.005      # sudden spike
DRIFT_PROB = 0.002      # start a slow drift episode
DRIFT_DURATION = 30     # cycles of drift


# ── Sensor state tracker ─────────────────────────────────────────────────────
@dataclass
class SensorState:
    sensor_id: str
    drift_remaining: int = 0
    drift_direction: float = 1.0
    drift_metric: str = "temperature"
    cycle: int = 0
    # Baseline offsets that slowly wander (realistic sensor aging)
    temp_offset: float = 0.0
    vib_offset: float = 0.0
    pres_offset: float = 0.0

    def tick(self) -> None:
        self.cycle += 1
        # Slow baseline wander (very gradual drift — realistic)
        self.temp_offset += random.gauss(0, 0.002)
        self.vib_offset += random.gauss(0, 0.0001)
        self.pres_offset += random.gauss(0, 0.005)


@dataclass
class SensorEvent:
    sensor_id: str
    timestamp: str
    temperature: float
    vibration: float
    pressure: float
    anomaly_injected: Optional[str] = None  # "spike" | "drift" | None

    def to_json(self) -> str:
        return json.dumps(asdict(self))


# ── Kafka producer ───────────────────────────────────────────────────────────
def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "linger.ms": 10,
        "compression.type": "lz4",
    })


def delivery_callback(err, msg):
    if err:
        log.error("Delivery failed for sensor %s: %s", msg.key(), err)


# ── Ensure topics exist ───────────────────────────────────────────────────────
def ensure_topics(broker: str, topic: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": broker})
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        log.info("Creating topic '%s' with %d partitions", topic, partitions)
        fs = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
        for t, f in fs.items():
            try:
                f.result()
                log.info("Topic '%s' created.", t)
            except Exception as exc:
                log.warning("Could not create topic '%s': %s", t, exc)
    else:
        log.info("Topic '%s' already exists.", topic)


# ── Value generation ─────────────────────────────────────────────────────────
def generate_reading(state: SensorState) -> SensorEvent:
    """Generate a single sensor reading, potentially injecting anomalies."""
    p = SENSOR_PROFILE

    # Normal readings with Gaussian noise
    temp = random.gauss(p["temperature"]["mean"] + state.temp_offset, p["temperature"]["std"])
    vib  = random.gauss(p["vibration"]["mean"]   + state.vib_offset,  p["vibration"]["std"])
    pres = random.gauss(p["pressure"]["mean"]    + state.pres_offset, p["pressure"]["std"])

    anomaly_type: Optional[str] = None

    # ── Spike injection (rare, immediate, large)
    if state.drift_remaining == 0 and random.random() < SPIKE_PROB:
        metric = random.choice(["temperature", "vibration", "pressure"])
        magnitude = random.uniform(4, 8)  # 4–8 standard deviations
        if metric == "temperature":
            temp += p["temperature"]["std"] * magnitude * random.choice([-1, 1])
        elif metric == "vibration":
            vib  += p["vibration"]["std"]   * magnitude * abs(random.gauss(1, 0.2))
        else:
            pres += p["pressure"]["std"]    * magnitude * random.choice([-1, 1])
        anomaly_type = "spike"
        log.debug("SPIKE injected on %s: %s", state.sensor_id, metric)

    # ── Drift injection (sustained cumulative shift)
    elif state.drift_remaining > 0:
        fraction = (DRIFT_DURATION - state.drift_remaining) / DRIFT_DURATION
        drift_magnitude = 0.15 * fraction  # gradually ramps up
        if state.drift_metric == "temperature":
            temp += state.drift_direction * p["temperature"]["std"] * drift_magnitude * DRIFT_DURATION
        elif state.drift_metric == "vibration":
            vib  += state.drift_direction * p["vibration"]["std"]   * drift_magnitude * DRIFT_DURATION
        else:
            pres += state.drift_direction * p["pressure"]["std"]    * drift_magnitude * DRIFT_DURATION
        state.drift_remaining -= 1
        anomaly_type = "drift"

    # ── Start new drift episode
    elif random.random() < DRIFT_PROB:
        state.drift_remaining = DRIFT_DURATION
        state.drift_metric = random.choice(["temperature", "vibration", "pressure"])
        state.drift_direction = random.choice([-1.0, 1.0])

    # Clamp values to physically plausible range
    temp = max(-50.0, min(150.0, round(temp, 3)))
    vib  = max(0.0,   min(50.0,  round(abs(vib), 5)))
    pres = max(80.0,  min(130.0, round(pres, 3)))

    return SensorEvent(
        sensor_id=state.sensor_id,
        timestamp=datetime.now(timezone.utc).isoformat(),
        temperature=temp,
        vibration=vib,
        pressure=pres,
        anomaly_injected=anomaly_type,
    )


# ── Main loop ────────────────────────────────────────────────────────────────
def run() -> None:
    log.info("Connecting to Kafka broker: %s", KAFKA_BROKER)

    # Wait for Kafka to be ready
    for attempt in range(30):
        try:
            ensure_topics(KAFKA_BROKER, TOPIC_RAW, PARTITIONS)
            break
        except Exception as exc:
            log.warning("Kafka not ready yet (attempt %d/30): %s", attempt + 1, exc)
            time.sleep(5)
    else:
        log.error("Could not connect to Kafka after 30 attempts. Exiting.")
        sys.exit(1)

    producer = make_producer()
    states = {sid: SensorState(sensor_id=sid) for sid in SENSORS}

    # Graceful shutdown
    running = True
    def _shutdown(sig, frame):
        nonlocal running
        log.info("Shutting down generator...")
        running = False
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("Publishing events for sensors: %s", SENSORS)
    published = 0

    while running:
        cycle_start = time.monotonic()

        for sensor_id, state in states.items():
            state.tick()
            event = generate_reading(state)
            payload = event.to_json().encode("utf-8")
            # Partition by sensor_id so per-sensor ordering is preserved
            producer.produce(
                topic=TOPIC_RAW,
                key=sensor_id.encode("utf-8"),
                value=payload,
                callback=delivery_callback,
            )
            published += 1

        producer.poll(0)  # trigger delivery callbacks without blocking

        if published % 100 == 0:
            log.info("Published %d events total.", published)

        # Sleep remainder of cycle to maintain ~PUBLISH_INTERVAL_S per sensor
        elapsed = time.monotonic() - cycle_start
        sleep_time = max(0.0, PUBLISH_INTERVAL_S - elapsed)
        time.sleep(sleep_time)

    producer.flush(timeout=10)
    log.info("Generator stopped. Total events published: %d", published)


if __name__ == "__main__":
    run()
