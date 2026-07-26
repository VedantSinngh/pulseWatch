"""
PulseWatch — /events router
GET /events — query raw enriched events from OpenSearch
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from models.schemas import DegradedResponse, EventListResponse
from services import opensearch_client as os_client
from services.opensearch_client import DegradedError

import os

INDEX = os.getenv("OPENSEARCH_INDEX_RAW", "events-raw")

router = APIRouter(prefix="/events", tags=["events"])


@router.get("", response_model=EventListResponse)
async def get_events(
    entity_id: Optional[str]      = Query(None, description="Sensor/entity ID"),
    since:     Optional[datetime] = Query(None, description="ISO 8601 start time"),
    until:     Optional[datetime] = Query(None, description="ISO 8601 end time"),
    anomalies_only: bool          = Query(False, description="Return only anomalous events"),
    limit:     int                = Query(200, ge=1, le=2000),
):
    """Returns raw enriched events with computed stats and anomaly flags."""
    must_clauses = []

    if entity_id:
        must_clauses.append({"term": {"sensor_id": entity_id}})

    if since or until:
        rng: dict = {"range": {"timestamp": {}}}
        if since:
            rng["range"]["timestamp"]["gte"] = since.isoformat()
        if until:
            rng["range"]["timestamp"]["lte"] = until.isoformat()
        must_clauses.append(rng)

    if anomalies_only:
        must_clauses.append({"term": {"is_anomaly": True}})

    query_body = {
        "query": {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}},
        "sort": [{"timestamp": {"order": "asc"}}],
    }

    try:
        resp = await os_client.search(INDEX, query_body, size=limit)
    except DegradedError as exc:
        return JSONResponse(
            status_code=503,
            content=DegradedResponse(
                error="opensearch_unavailable",
                message=str(exc),
            ).model_dump(mode="json"),
        )

    hits = resp.get("hits", {})
    total = hits.get("total", {}).get("value", 0)
    records = [h.get("_source", {}) for h in hits.get("hits", [])]

    return EventListResponse(total=total, events=records)
