"""
BQ-123A: FastAPI middleware for request/correlation ID injection.

Injects request_id and correlation_id into contextvars so structlog
processors automatically include them in every log entry.
"""
from __future__ import annotations

import time
import uuid
import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from app.core.structured_logging import (
    request_id_var,
    correlation_id_var,
)

logger = logging.getLogger(__name__)


class CorrelationMiddleware(BaseHTTPMiddleware):
    """Inject request_id / correlation_id into contextvars for every request."""

    async def dispatch(self, request: Request, call_next) -> Response:
        # Accept inbound headers or generate UUIDs
        req_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        corr_id = request.headers.get("x-correlation-id") or uuid.uuid4().hex

        # Set contextvars (structlog processors pick these up)
        rid_token = request_id_var.set(req_id)
        cid_token = correlation_id_var.set(corr_id)

        start = time.perf_counter()
        response = None
        try:
            response = await call_next(request)
        except Exception:
            raise
        finally:
            duration_ms = round((time.perf_counter() - start) * 1000, 2)
            status_code = response.status_code if response else None
            logger.info(
                "request_completed",
                extra={
                    "http.method": request.method,
                    "http.path_template": request.url.path,
                    "http.status_code": status_code,
                    "duration_ms": duration_ms,
                },
            )
            # Reset contextvars
            request_id_var.reset(rid_token)
            correlation_id_var.reset(cid_token)

        # Echo IDs back in response headers
        response.headers["x-request-id"] = req_id
        response.headers["x-correlation-id"] = corr_id
        return response


def generate_ws_session_context() -> tuple[str, str]:
    """Generate session_id + correlation_id for a WebSocket connection.

    Returns (session_id, correlation_id).
    The caller should set session_id_var and correlation_id_var.
    """
    return uuid.uuid4().hex, uuid.uuid4().hex
