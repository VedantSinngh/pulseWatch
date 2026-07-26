"""
PulseWatch — Kafka anomaly consumer + WebSocket broadcast manager.

Uses aiokafka AsyncConsumer running as a background asyncio task.
Maintains a set of active WebSocket connections; when a new anomaly
arrives from Kafka it is broadcast to all connected clients.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Set

from fastapi import WebSocket
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

log = logging.getLogger("backend.kafka_consumer")

KAFKA_BROKER         = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_ANOMALIES      = os.getenv("KAFKA_TOPIC_ANOMALIES", "anomaly-alerts")
CONSUMER_GROUP       = "pulsewatch-fastapi-ws"

# ── WebSocket connection registry ─────────────────────────────────────────────
_connections: Set[WebSocket] = set()
_consumer_task: asyncio.Task | None = None


def register(ws: WebSocket) -> None:
    _connections.add(ws)
    log.info("WebSocket registered. Active connections: %d", len(_connections))


def unregister(ws: WebSocket) -> None:
    _connections.discard(ws)
    log.info("WebSocket unregistered. Active connections: %d", len(_connections))


async def broadcast(message: str) -> None:
    """Send a message to all active WebSocket clients, removing dead ones."""
    dead: Set[WebSocket] = set()
    for ws in list(_connections):
        try:
            await ws.send_text(message)
        except Exception:
            dead.add(ws)
    for ws in dead:
        _connections.discard(ws)


# ── Kafka consumer loop ───────────────────────────────────────────────────────
async def _consume_loop() -> None:
    """
    Continuously consume from anomaly-alerts Kafka topic and broadcast
    each message to all connected WebSocket clients.
    Retries indefinitely on connection failure with backoff.
    """
    backoff = 2.0
    while True:
        consumer: AIOKafkaConsumer | None = None
        try:
            consumer = AIOKafkaConsumer(
                TOPIC_ANOMALIES,
                bootstrap_servers=KAFKA_BROKER,
                group_id=CONSUMER_GROUP,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda v: v.decode("utf-8"),
                # Reasonable session timeout for local dev
                session_timeout_ms=30000,
                heartbeat_interval_ms=3000,
            )
            await consumer.start()
            log.info("Kafka consumer started. Subscribed to: %s", TOPIC_ANOMALIES)
            backoff = 2.0  # reset backoff on successful connect

            async for msg in consumer:
                try:
                    payload = json.loads(msg.value)
                    await broadcast(json.dumps(payload))
                    log.debug("Broadcast anomaly: sensor=%s", payload.get("sensor_id"))
                except json.JSONDecodeError as exc:
                    log.warning("Invalid JSON from Kafka: %s", exc)
                except Exception as exc:
                    log.warning("Error processing Kafka message: %s", exc)

        except asyncio.CancelledError:
            log.info("Kafka consumer task cancelled.")
            break
        except Exception as exc:
            log.warning("Kafka consumer error: %s. Retrying in %.0fs...", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)  # exponential backoff, cap at 60s
        finally:
            if consumer is not None:
                try:
                    await consumer.stop()
                except Exception:
                    pass


async def start_consumer() -> None:
    """Launch the consumer loop as a background asyncio task."""
    global _consumer_task
    _consumer_task = asyncio.create_task(_consume_loop(), name="kafka-anomaly-consumer")
    log.info("Kafka anomaly consumer task started.")


async def stop_consumer() -> None:
    """Cancel the consumer task on app shutdown."""
    global _consumer_task
    if _consumer_task and not _consumer_task.done():
        _consumer_task.cancel()
        try:
            await _consumer_task
        except asyncio.CancelledError:
            pass
    log.info("Kafka anomaly consumer task stopped.")


async def check_health() -> dict:
    """Try to reach Kafka and return health dict."""
    t0 = time.monotonic()
    try:
        probe = AIOKafkaConsumer(
            bootstrap_servers=KAFKA_BROKER,
            request_timeout_ms=3000,
        )
        await asyncio.wait_for(probe.start(), timeout=4.0)
        await probe.stop()
        latency = (time.monotonic() - t0) * 1000
        return {"status": "ok", "latency_ms": round(latency, 1)}
    except Exception as exc:
        latency = (time.monotonic() - t0) * 1000
        return {"status": "down", "latency_ms": round(latency, 1), "message": str(exc)}
