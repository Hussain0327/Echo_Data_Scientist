import time
from typing import Callable

import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = structlog.get_logger()


class TelemetryMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()

        method = request.method
        path = request.url.path

        try:
            response = await call_next(request)
            duration = time.time() - start_time

            logger.info(
                "request_completed",
                method=method,
                path=path,
                status_code=response.status_code,
                duration_ms=round(duration * 1000, 2),
            )

            response.headers["X-Process-Time"] = str(duration)
            return response

        except Exception as e:
            duration = time.time() - start_time

            logger.error(
                "request_failed",
                method=method,
                path=path,
                error=str(e),
                duration_ms=round(duration * 1000, 2),
            )
            raise
