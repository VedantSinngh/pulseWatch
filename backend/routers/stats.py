"""
PulseWatch — /stats and /health routers
GET /stats/{entity_id} — per-sensor rolling stats
GET /health            — system health check
GET /sensors           — list known sensor IDs
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Path
from fastapi.responses import JSONResponse

from models.schemas import (
    ComponentHealth, DegradedResponse, HealthResponse,
    SensorStats, ServiceStatus,
)
from services import opensearch_client as os_client
from services import kafka_consumer
from services.opensearch_client import DegradedError

import os

INDEX_RAW       = os.getenv("OPENSEARCH_INDEX_RAW",       "events-raw")
INDEX_ANOMALIES = os.getenv("OPENSEARCH_INDEX_ANOMALIES", "events-anomalies")

router = APIRouter(tags=["stats"])


# ── GET /sensors ──────────────────────────────────────────────────────────────
@router.get("/sensors")
async def list_sensors():
    """Return a list of all distinct sensor IDs seen in events-raw."""
    body = {
        "size": 0,
        "aggs": {
            "sensors": {
                "terms": {"field": "sensor_id", "size": 100}
            }
        },
    }
    try:
        resp = await os_client.search(INDEX_RAW, body, size=0)
        buckets = resp.get("aggregations", {}).get("sensors", {}).get("buckets", [])
        return {"sensors": [b["key"] for b in buckets]}
    except DegradedError:
        # Return known defaults so the frontend isn't stuck on first boot
        return {"sensors": [f"sensor-{i:03d}" for i in range(1, 6)]}


# ── GET /stats/{entity_id} ────────────────────────────────────────────────────
@router.get("/stats/{entity_id}", response_model=SensorStats)
async def get_stats(
    entity_id: str = Path(..., description="Sensor/entity ID"),
):
    """Returns rolling stats and anomaly count for a specific sensor."""
    one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

    # Latest event
    latest_body = {
        "query": {"term": {"sensor_id": entity_id}},
        "sort": [{"timestamp": {"order": "desc"}}],
        "size": 1,
    }
    # Mean over last hour
    stats_body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"sensor_id": entity_id}},
                    {"range": {"timestamp": {"gte": one_hour_ago}}},
                ]
            }
        },
        "size": 0,
        "aggs": {
            "avg_temperature": {"avg": {"field": "temperature"}},
            "avg_vibration":   {"avg": {"field": "vibration"}},
            "avg_pressure":    {"avg": {"field": "pressure"}},
        },
    }
    # Anomaly count last hour
    anomaly_count_body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"sensor_id": entity_id}},
                    {"range": {"timestamp": {"gte": one_hour_ago}}},
                ]
            }
        },
    }

    try:
        latest_resp = await os_client.search(INDEX_RAW, latest_body, size=1)
        stats_resp  = await os_client.search(INDEX_RAW, stats_body, size=0)
        anom_count  = await os_client.count(INDEX_ANOMALIES, anomaly_count_body)
    except DegradedError as exc:
        return JSONResponse(
            status_code=503,
            content=DegradedResponse(
                error="opensearch_unavailable",
                message=str(exc),
            ).model_dump(mode="json"),
        )

    latest_hits = latest_resp.get("hits", {}).get("hits", [])
    latest_src = latest_hits[0].get("_source", {}) if latest_hits else {}

    aggs = stats_resp.get("aggregations", {})
    mean_temp = (aggs.get("avg_temperature") or {}).get("value")
    mean_vib  = (aggs.get("avg_vibration")   or {}).get("value")
    mean_pres = (aggs.get("avg_pressure")    or {}).get("value")

    return SensorStats(
        sensor_id=entity_id,
        latest_timestamp=latest_src.get("timestamp"),
        latest_temperature=latest_src.get("temperature"),
        latest_vibration=latest_src.get("vibration"),
        latest_pressure=latest_src.get("pressure"),
        mean_temperature=round(mean_temp, 3) if mean_temp is not None else None,
        mean_vibration=round(mean_vib, 5)    if mean_vib  is not None else None,
        mean_pressure=round(mean_pres, 3)    if mean_pres is not None else None,
        anomaly_count_last_hour=anom_count,
        latest_z_temperature=latest_src.get("z_temperature"),
        latest_z_vibration=latest_src.get("z_vibration"),
        latest_z_pressure=latest_src.get("z_pressure"),
    )


# ── GET /health ───────────────────────────────────────────────────────────────
@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Returns system health: checks OpenSearch and Kafka connectivity.
    Always returns 200 with a structured status — never raises 500.
    """
    import asyncio
    os_health, kafka_health = await asyncio.gather(
        os_client.check_health(),
        kafka_consumer.check_health(),
    )

    os_status    = ServiceStatus.ok if os_health["status"] == "ok"    else ServiceStatus.down
    kafka_status = ServiceStatus.ok if kafka_health["status"] == "ok"  else ServiceStatus.down

    overall = ServiceStatus.ok
    if os_status == ServiceStatus.down or kafka_status == ServiceStatus.down:
        overall = ServiceStatus.degraded

    return HealthResponse(
        status=overall,
        opensearch=ComponentHealth(
            status=os_status,
            latency_ms=os_health.get("latency_ms"),
            message=os_health.get("message"),
        ),
        kafka=ComponentHealth(
            status=kafka_status,
            latency_ms=kafka_health.get("latency_ms"),
            message=kafka_health.get("message"),
        ),
        timestamp=datetime.now(timezone.utc),
    )
