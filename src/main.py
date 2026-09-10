"""FastAPI application entry point.

Creates the app with:
- Lifespan context manager for Redis + httpx client lifecycle.
- Rate limit middleware.
- Dashboard routes (mounted first so they take priority).
- Proxy catch-all route (mounted last).

Author: Pushkar Reddy (@pushkarreddyy)
Repository: https://github.com/pushkarreddyy/api-rate-limiter-proxy
"""

import logging
from contextlib import asynccontextmanager

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI

from src.config import get_settings
from src.rate_limiter import SlidingWindowRateLimiter
from src.analytics import AnalyticsService
from src.middleware import RateLimitMiddleware
from src.dashboard import router as dashboard_router
from src.proxy import router as proxy_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown.

    On startup:
      - Connect to Redis and store on app.state.
      - Create the httpx AsyncClient for upstream proxying.
      - Initialise the rate limiter and analytics service.

    On shutdown:
      - Gracefully close the httpx client and Redis connection.
    """
    settings = get_settings()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("proxy")
    logger.info("Starting API Rate Limiter & Reverse Proxy")
    logger.info("Upstream: %s", settings.UPSTREAM_BASE_URL)
    logger.info("Rate limit: %d requests / %ds window",
                settings.RATE_LIMIT_MAX_REQUESTS, settings.RATE_LIMIT_WINDOW_SECONDS)

    # --- Redis ---
    redis_client = aioredis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        max_connections=50,
    )
    # Verify connection
    try:
        await redis_client.ping()
        logger.info("Connected to Redis at %s", settings.REDIS_URL)
    except Exception as exc:
        logger.error("Failed to connect to Redis: %s", exc)
        raise

    # --- httpx client for upstream requests ---
    http_client = httpx.AsyncClient(
        base_url=settings.UPSTREAM_BASE_URL,
        limits=httpx.Limits(max_keepalive_connections=30, max_connections=100),
        timeout=httpx.Timeout(
            connect=5.0,
            read=settings.PROXY_TIMEOUT_SECONDS,
            write=settings.PROXY_TIMEOUT_SECONDS,
            pool=5.0,
        ),
        follow_redirects=False,
    )

    # --- Services ---
    app.state.redis = redis_client
    app.state.http_client = http_client
    app.state.rate_limiter = SlidingWindowRateLimiter(
        redis_client=redis_client,
        max_requests=settings.RATE_LIMIT_MAX_REQUESTS,
        window_seconds=settings.RATE_LIMIT_WINDOW_SECONDS,
    )
    app.state.analytics = AnalyticsService(redis_client=redis_client)

    logger.info("Proxy gateway ready on http://localhost:8000")
    logger.info("Dashboard at http://localhost:8000/dashboard")

    yield

    # --- Shutdown ---
    logger.info("Shutting down...")
    await http_client.aclose()
    await redis_client.aclose()
    logger.info("Goodbye.")


# --- Create the FastAPI app ---
app = FastAPI(
    title="API Rate Limiter & Reverse Proxy",
    description="A lightweight gateway that rate-limits clients, forwards requests "
                "to upstream services, and provides real-time analytics.",
    version="1.0.0",
    lifespan=lifespan,
    docs_url=None,   # Disable default docs (our proxy catch-all would conflict)
    redoc_url=None,
)

# 1. Add rate limiting middleware (runs on every request)
app.add_middleware(RateLimitMiddleware)

# 2. Mount dashboard routes FIRST (so /dashboard/* isn't caught by the proxy)
app.include_router(dashboard_router)

# 3. Mount proxy catch-all route LAST
app.include_router(proxy_router)
