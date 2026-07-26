"""
PulseWatch — /anomalies router
GET /anomalies — query anomaly events from OpenSearch
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Query, Response
from fastapi.responses import JSONResponse

from models.schemas import AnomalyListResponse, DegradedResponse
from services import opensearch_client as os_client
from services.opensearch_client import DegradedError

import os

INDEX = os.getenv("OPENSEARCH_INDEX_ANOMALIES", "events-anomalies")

router = APIRouter(prefix="/anomalies", tags=["anomalies"])


@router.get("", response_model=AnomalyListResponse)
async def get_anomalies(
    since:     Optional[datetime] = Query(None, description="ISO 8601 start time"),
    until:     Optional[datetime] = Query(None, description="ISO 8601 end time"),
    entity_id: Optional[str]      = Query(None, alias="entity_id",
                                          description="Sensor/entity ID to filter by"),
    severity:  Optional[str]      = Query(None, description="Filter by severity: low|medium|high"),
    limit:     int                = Query(100, ge=1, le=1000, description="Max results"),
):
    """
    Returns a list of anomaly records. All filters are optional.
    Returns a 503 degraded response if OpenSearch is unreachable.
    """
    must_clauses = []

    if since or until:
        range_filter: dict = {"range": {"timestamp": {}}}
        if since:
            range_filter["range"]["timestamp"]["gte"] = since.isoformat()
        if until:
            range_filter["range"]["timestamp"]["lte"] = until.isoformat()
        must_clauses.append(range_filter)

    if entity_id:
        must_clauses.append({"term": {"sensor_id": entity_id}})

    if severity:
        must_clauses.append({"term": {"severity": severity}})

    query_body = {
        "query": {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}},
        "sort": [{"timestamp": {"order": "desc"}}],
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
    records = []
    for hit in hits.get("hits", []):
        src = hit.get("_source", {})
        try:
            records.append(src)
        except Exception:
            pass

    return AnomalyListResponse(total=total, anomalies=records)
