"""
PulseWatch — Pydantic v2 Schemas
All request/response models with validation.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class Severity(str, Enum):
    low    = "low"
    medium = "medium"
    high   = "high"


class AnomalyRecord(BaseModel):
    sensor_id:        str
    timestamp:        datetime
    temperature:      float
    vibration:        float
    pressure:         float
    z_temperature:    Optional[float] = None
    z_vibration:      Optional[float] = None
    z_pressure:       Optional[float] = None
    anomaly_types:    List[str] = Field(default_factory=list)
    severity:         Optional[str] = None
    if_score:         Optional[float] = None
    processed_at:     Optional[datetime] = None


class AnomalyListResponse(BaseModel):
    total:     int
    anomalies: List[AnomalyRecord]


class EventRecord(BaseModel):
    sensor_id:    str
    timestamp:    datetime
    temperature:  float
    vibration:    float
    pressure:     float
    is_anomaly:   Optional[bool]   = False
    anomaly_types: List[str]       = Field(default_factory=list)
    processed_at: Optional[datetime] = None


class EventListResponse(BaseModel):
    total:  int
    events: List[EventRecord]


class SensorStats(BaseModel):
    sensor_id:        str
    latest_timestamp: Optional[datetime] = None
    latest_temperature: Optional[float]  = None
    latest_vibration:   Optional[float]  = None
    latest_pressure:    Optional[float]  = None
    mean_temperature:   Optional[float]  = None
    mean_vibration:     Optional[float]  = None
    mean_pressure:      Optional[float]  = None
    anomaly_count_last_hour: int = 0
    latest_z_temperature: Optional[float] = None
    latest_z_vibration:   Optional[float] = None
    latest_z_pressure:    Optional[float] = None


class ServiceStatus(str, Enum):
    ok       = "ok"
    degraded = "degraded"
    down     = "down"


class ComponentHealth(BaseModel):
    status:  ServiceStatus
    latency_ms: Optional[float] = None
    message: Optional[str]      = None


class HealthResponse(BaseModel):
    status:       ServiceStatus
    opensearch:   ComponentHealth
    kafka:        ComponentHealth
    timestamp:    datetime


class DegradedResponse(BaseModel):
    """Returned instead of crashing when a backend dependency is unreachable."""
    error:   str
    message: str
    status:  str = "degraded"
