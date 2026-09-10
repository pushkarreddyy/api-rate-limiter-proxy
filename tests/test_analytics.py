"""Unit tests for the Analytics service.

Author: @pushkarreddyy
Repository: https://github.com/pushkarreddyy/api-rate-limiter-proxy
"""

import pytest
import pytest_asyncio
from src.analytics import AnalyticsService
from tests.test_all import SimpleInMemoryRedis


@pytest_asyncio.fixture
async def fake_redis():
    """Create an in-memory Redis client."""
    client = SimpleInMemoryRedis()
    yield client
    await client.aclose()


@pytest.mark.asyncio
async def test_analytics_recording_and_summary(fake_redis):
    """Verify that requests are recorded and summary stats are accurately calculated."""
    analytics = AnalyticsService(fake_redis)

    # Record some sample requests
    await analytics.record("user_1", "GET", "/api/v1/users", 200, 15.4)
    await analytics.record("user_1", "POST", "/api/v1/users", 201, 25.0)
    await analytics.record("user_2", "GET", "/api/v1/users", 429, 2.1)
    await analytics.record("user_3", "GET", "/api/v1/error", 500, 45.0)

    summary = await analytics.get_summary()
    assert summary["total_requests"] == 4
    assert summary["unique_consumers"] == 3
    assert summary["avg_latency_ms"] > 0

    errors = await analytics.get_error_breakdown()
    assert errors["2xx"] == 2
    assert errors["4xx"] == 1
    assert errors["5xx"] == 1

    top = await analytics.get_top_consumers(limit=5)
    assert len(top) == 3
    assert top[0]["client_id"] == "user_1"
    assert top[0]["request_count"] == 2

    recent = await analytics.get_recent_requests(limit=10)
    assert len(recent) == 4
