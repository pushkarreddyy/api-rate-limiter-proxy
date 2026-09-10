"""Unit tests for the Sliding Window Log rate limiter.

Author: @pushkarreddyy
Repository: https://github.com/pushkarreddyy/api-rate-limiter-proxy
"""

import asyncio
import pytest
import pytest_asyncio
from src.rate_limiter import SlidingWindowRateLimiter
from tests.test_all import SimpleInMemoryRedis


@pytest_asyncio.fixture
async def fake_redis():
    """Create an in-memory Redis client for isolated testing."""
    client = SimpleInMemoryRedis()
    yield client
    await client.aclose()


@pytest.mark.asyncio
async def test_sliding_window_allows_within_limit(fake_redis):
    """Verify that requests within quota are allowed and remaining count decreases."""
    limiter = SlidingWindowRateLimiter(fake_redis, max_requests=5, window_seconds=10)

    for i in range(5):
        result = await limiter.check("client_test_1")
        assert result.allowed is True
        assert result.remaining == (4 - i)
        assert result.limit == 5
        assert result.retry_after is None


@pytest.mark.asyncio
async def test_sliding_window_blocks_over_limit(fake_redis):
    """Verify that the (N+1)th request is denied with 429 semantics and retry_after."""
    limiter = SlidingWindowRateLimiter(fake_redis, max_requests=3, window_seconds=10)

    # Exhaust limit (3 requests)
    for _ in range(3):
        res = await limiter.check("client_test_2")
        assert res.allowed is True

    # 4th request must be blocked
    blocked = await limiter.check("client_test_2")
    assert blocked.allowed is False
    assert blocked.remaining == 0
    assert blocked.retry_after is not None
    assert blocked.retry_after > 0


@pytest.mark.asyncio
async def test_independent_client_quotas(fake_redis):
    """Verify that different clients have isolated rate limit quotas."""
    limiter = SlidingWindowRateLimiter(fake_redis, max_requests=2, window_seconds=10)

    # Client A exhausts quota
    await limiter.check("client_A")
    await limiter.check("client_A")
    res_a = await limiter.check("client_A")
    assert res_a.allowed is False

    # Client B should still have full quota
    res_b = await limiter.check("client_B")
    assert res_b.allowed is True
    assert res_b.remaining == 1
