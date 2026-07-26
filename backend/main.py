"""
PulseWatch — FastAPI Application Entry Point
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pythonjsonlogger import jsonlogger

from routers import anomalies, events, stats
from services import kafka_consumer, opensearch_client

# ── Structured JSON logging ───────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
handler.setFormatter(formatter)

root_logger = logging.getLogger()
root_logger.handlers.clear()
root_logger.addHandler(handler)
root_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# Quiet noisy libraries
logging.getLogger("aiokafka").setLevel(logging.WARNING)
logging.getLogger("opensearchpy").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

log = logging.getLogger("backend.main")


# ── App lifespan ──────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: initialise OpenSearch client and Kafka consumer."""
    log.info("PulseWatch backend starting up...")
    # Eagerly create the OS client (validates config, not connectivity)
    opensearch_client.get_client()
    # Start Kafka → WebSocket consumer background task
    await kafka_consumer.start_consumer()
    log.info("PulseWatch backend ready.")
    yield
    # Shutdown
    log.info("PulseWatch backend shutting down...")
    await kafka_consumer.stop_consumer()
    await opensearch_client.close_client()
    log.info("PulseWatch backend stopped.")


# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="PulseWatch API",
    description="Real-time streaming analytics and anomaly detection platform",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Allow Next.js frontend (any origin for local dev)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(anomalies.router)
app.include_router(events.router)
app.include_router(stats.router)


# ── WebSocket endpoint ────────────────────────────────────────────────────────
@app.websocket("/ws/anomalies")
async def websocket_anomalies(websocket: WebSocket):
    """
    WebSocket endpoint — clients connect here to receive real-time anomaly alerts.
    The Kafka consumer background task calls broadcast() for each new anomaly.
    """
    await websocket.accept()
    kafka_consumer.register(websocket)
    log.info("New WebSocket client connected.")
    try:
        # Keep the connection open; the consumer task pushes messages
        while True:
            # Accept any ping/pong or control messages from the client
            data = await websocket.receive_text()
            # Optionally echo back (e.g. for heartbeat)
            if data.strip().lower() == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        log.info("WebSocket client disconnected.")
    finally:
        kafka_consumer.unregister(websocket)


# ── Root ──────────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "service": "PulseWatch API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
        "websocket": "ws://<host>:8000/ws/anomalies",
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("BACKEND_PORT", "8000")),
        reload=False,
        log_config=None,  # Use our own logging config
    )
