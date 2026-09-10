"""Rate limiting and analytics middleware.

Runs before every proxied request to:
1. Identify the client (API key or IP address).
2. Check the sliding window rate limit.
3. Return 429 if the client has exceeded their quota.
4. After the response is produced, record analytics.

Author: Pushkar Reddy (@pushkarreddyy)
Repository: https://github.com/pushkarreddyy/api-rate-limiter-proxy
"""

import time
import logging
from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse

from src.rate_limiter import SlidingWindowRateLimiter, RateLimitResult
from src.analytics import AnalyticsService

logger = logging.getLogger("proxy.middleware")


def _get_client_id(request: Request) -> str:
    """Extract client identifier from the request.

    Priority:
    1. X-API-Key header (authenticated clients get their own quota).
    2. Client IP address (from X-Forwarded-For or direct connection).
    """
    api_key = request.headers.get("x-api-key")
    if api_key:
        return f"key:{api_key}"

    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return f"ip:{forwarded_for.split(',')[0].strip()}"

    if request.client:
        return f"ip:{request.client.host}"

    return "ip:unknown"


def _build_rate_limit_headers(result: RateLimitResult) -> dict[str, str]:
    """Build the standard rate limit headers from a check result."""
    headers = {
        "X-RateLimit-Limit": str(result.limit),
        "X-RateLimit-Remaining": str(result.remaining),
        "X-RateLimit-Reset": str(int(result.reset_at)),
    }
    if result.retry_after is not None:
        headers["Retry-After"] = str(int(result.retry_after) + 1)
    return headers


class RateLimitMiddleware(BaseHTTPMiddleware):
    """ASGI middleware that enforces per-client rate limits.

    Intercepts every request (except dashboard routes), checks the rate
    limit against Redis, and either allows the request through or returns
    a 429 response with appropriate headers.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip rate limiting for dashboard and health-check routes
        if request.url.path.startswith("/dashboard"):
            return await call_next(request)

        rate_limiter: SlidingWindowRateLimiter = request.app.state.rate_limiter
        analytics: AnalyticsService = request.app.state.analytics

        client_id = _get_client_id(request)
        request.state.client_id = client_id

        # --- Check rate limit ---
        result = await rate_limiter.check(client_id)

        if not result.allowed:
            logger.info(
                "Rate limited client=%s remaining=%d retry_after=%.1f",
                client_id, result.remaining, result.retry_after or 0,
            )
            headers = _build_rate_limit_headers(result)
            retry_after = int(result.retry_after or 1)

            # Record the 429 in analytics
            await analytics.record(
                client_id=client_id,
                method=request.method,
                path=request.url.path,
                status_code=429,
                latency_ms=0,
            )

            return JSONResponse(
                status_code=429,
                content={
                    "error": "rate_limit_exceeded",
                    "message": f"Too many requests. Try again in {retry_after} seconds.",
                    "retry_after": retry_after,
                },
                headers=headers,
            )

        # --- Request is allowed — forward to proxy ---
        start_time = time.perf_counter()
        response = await call_next(request)
        latency_ms = (time.perf_counter() - start_time) * 1000

        # Inject rate limit headers into the proxied response
        rl_headers = _build_rate_limit_headers(result)
        for key, value in rl_headers.items():
            response.headers[key] = value

        # Determine the upstream status code
        status_code = getattr(request.state, "upstream_status", response.status_code)

        # Record analytics (non-blocking, fire-and-forget)
        try:
            await analytics.record(
                client_id=client_id,
                method=request.method,
                path=request.url.path,
                status_code=status_code,
                latency_ms=latency_ms,
            )
        except Exception:
            logger.exception("Failed to record analytics")

        return response
