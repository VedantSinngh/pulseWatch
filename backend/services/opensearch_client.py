"""
PulseWatch — Async OpenSearch client wrapper
Wraps opensearch-py AsyncOpenSearch with graceful degradation:
every method returns a result or raises DegradedError instead of crashing.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, List, Optional

from opensearchpy import AsyncOpenSearch, ConnectionError as OSConnectionError
from opensearchpy import NotFoundError, RequestError

log = logging.getLogger("backend.opensearch")

OS_HOST   = os.getenv("OPENSEARCH_HOST",   "opensearch")
OS_PORT   = int(os.getenv("OPENSEARCH_PORT", "9200"))
OS_SCHEME = os.getenv("OPENSEARCH_SCHEME", "http")


class DegradedError(RuntimeError):
    """Raised when OpenSearch is unreachable so the router can return 503."""
    pass


# Singleton client — created once on app startup
_client: Optional[AsyncOpenSearch] = None


def get_client() -> AsyncOpenSearch:
    global _client
    if _client is None:
        _client = AsyncOpenSearch(
            hosts=[{"host": OS_HOST, "port": OS_PORT, "scheme": OS_SCHEME}],
            http_compress=True,
            use_ssl=False,
            verify_certs=False,
            timeout=10,
            max_retries=2,
            retry_on_timeout=True,
        )
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None


async def check_health() -> Dict[str, Any]:
    """Check OpenSearch cluster health. Returns dict with status and latency."""
    t0 = time.monotonic()
    try:
        client = get_client()
        resp = await client.cluster.health()
        latency = (time.monotonic() - t0) * 1000
        return {"status": "ok", "latency_ms": round(latency, 1), "cluster": resp}
    except Exception as exc:
        latency = (time.monotonic() - t0) * 1000
        log.warning("OpenSearch health check failed: %s", exc)
        return {"status": "down", "latency_ms": round(latency, 1), "message": str(exc)}


async def search(index: str, body: Dict, size: int = 100) -> Dict:
    """Execute a search query; raises DegradedError on connectivity failure."""
    try:
        client = get_client()
        resp = await client.search(index=index, body=body, size=size)
        return resp
    except OSConnectionError as exc:
        log.error("OpenSearch connection error on search [%s]: %s", index, exc)
        raise DegradedError(f"OpenSearch unreachable: {exc}") from exc
    except NotFoundError:
        # Index doesn't exist yet — return empty result instead of crashing
        return {"hits": {"total": {"value": 0}, "hits": []}}
    except Exception as exc:
        log.error("OpenSearch search error [%s]: %s", index, exc)
        raise DegradedError(str(exc)) from exc


async def count(index: str, body: Optional[Dict] = None) -> int:
    """Count documents in an index."""
    try:
        client = get_client()
        resp = await client.count(index=index, body=body or {"query": {"match_all": {}}})
        return int(resp.get("count", 0))
    except OSConnectionError as exc:
        log.warning("OpenSearch count failed: %s", exc)
        raise DegradedError(str(exc)) from exc
    except NotFoundError:
        return 0
    except Exception as exc:
        log.warning("OpenSearch count error: %s", exc)
        return 0
