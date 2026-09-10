"""Integration tests for the Reverse Proxy and Rate Limit Middleware.

Author: @pushkarreddyy
Repository: https://github.com/pushkarreddyy/api-rate-limiter-proxy
"""

import pytest
import pytest_asyncio
import httpx
from fastapi.testclient import TestClient

from src.main import app
from src.rate_limiter import SlidingWindowRateLimiter
from src.analytics import AnalyticsService
from mock_backend.server import app as mock_backend_app
from tests.test_all import SimpleInMemoryRedis


@pytest_asyncio.fixture
async def setup_app():
    """Setup app with in-memory redis and mock backend client."""
    redis_client = SimpleInMemoryRedis()

    # Use ASGITransport for in-memory upstream mock
    transport = httpx.ASGITransport(app=mock_backend_app)
    mock_client = httpx.AsyncClient(transport=transport, base_url="http://mock-backend")

    app.state.redis = redis_client
    app.state.http_client = mock_client
    app.state.rate_limiter = SlidingWindowRateLimiter(
        redis_client=redis_client,
        max_requests=5,
        window_seconds=10,
    )
    app.state.analytics = AnalyticsService(redis_client=redis_client)

    yield app

    await mock_client.aclose()
    await redis_client.aclose()


@pytest.mark.asyncio
async def test_proxy_forwarding_and_headers(setup_app):
    """Test that requests are forwarded to upstream and rate limit headers are attached."""
    transport = httpx.ASGITransport(app=setup_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testproxy") as client:
        resp = await client.get("/api/v1/users", headers={"X-API-Key": "test-key-1"})
        assert resp.status_code == 200
        data = resp.json()
        assert "users" in data
        assert resp.headers.get("x-ratelimit-limit") == "5"
        assert resp.headers.get("x-ratelimit-remaining") == "4"


@pytest.mark.asyncio
async def test_proxy_rate_limit_exceeded(setup_app):
    """Test that 429 is returned when quota (5 requests) is exceeded."""
    transport = httpx.ASGITransport(app=setup_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testproxy") as client:
        # Exhaust 5 requests
        for i in range(5):
            r = await client.get("/api/v1/users", headers={"X-API-Key": "burst-client"})
            assert r.status_code == 200

        # 6th request must receive 429
        blocked = await client.get("/api/v1/users", headers={"X-API-Key": "burst-client"})
        assert blocked.status_code == 429
        assert blocked.headers.get("x-ratelimit-remaining") == "0"
        assert "retry-after" in blocked.headers
        body = blocked.json()
        assert body["error"] == "rate_limit_exceeded"
