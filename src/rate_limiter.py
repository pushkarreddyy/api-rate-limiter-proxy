"""Sliding Window Log rate limiter backed by Redis.

Uses a Lua script for atomic check-and-set operations on Redis Sorted Sets.
Each request is logged as a member in a sorted set keyed by client ID,
with the score being the request timestamp.

Author: Pushkar Reddy (@pushkarreddyy)
Repository: https://github.com/pushkarreddyy/api-rate-limiter-proxy
"""

import time
import uuid
from dataclasses import dataclass

import redis.asyncio as redis


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""

    allowed: bool
    limit: int
    remaining: int
    reset_at: float
    retry_after: float | None = None


# Lua script for atomic sliding window log rate limiting.
# Ensures ZREMRANGEBYSCORE + ZCARD + conditional ZADD happen as one unit.
# KEYS[1] = rate limit key
# ARGV[1] = window_start (now - window_seconds)
# ARGV[2] = now (current timestamp)
# ARGV[3] = max_requests
# ARGV[4] = unique member value
# ARGV[5] = key TTL (window_seconds + 1)
#
# Returns: {allowed (0/1), remaining, reset_at, retry_after}
RATE_LIMIT_LUA = """
local key = KEYS[1]
local window_start = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local max_requests = tonumber(ARGV[3])
local member = ARGV[4]
local ttl = tonumber(ARGV[5])

-- Remove expired entries outside the window
redis.call('ZREMRANGEBYSCORE', key, 0, window_start)

-- Count current entries in the window
local current_count = redis.call('ZCARD', key)

if current_count < max_requests then
    -- Allowed: add this request to the log
    redis.call('ZADD', key, now, member)
    redis.call('EXPIRE', key, ttl)
    local remaining = max_requests - current_count - 1
    local reset_at = now + (ttl - 1)
    return {1, remaining, tostring(reset_at), "0"}
else
    -- Denied: calculate retry_after from the oldest entry
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local retry_after = 0
    if #oldest >= 2 then
        retry_after = tonumber(oldest[2]) + (ttl - 1) - now
        if retry_after < 0 then retry_after = 1 end
    end
    local reset_at = now + retry_after
    return {0, 0, tostring(reset_at), tostring(retry_after)}
end
"""


class SlidingWindowRateLimiter:
    """Rate limiter using the Sliding Window Log algorithm.

    Each request is recorded as a unique entry in a Redis Sorted Set.
    The score is the request timestamp. Expired entries (outside the
    sliding window) are pruned on each check.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        max_requests: int = 100,
        window_seconds: int = 60,
    ) -> None:
        self.redis = redis_client
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._script: redis.client.Script | None = None

    async def _get_script(self) -> redis.client.Script:
        """Lazily register the Lua script with Redis."""
        if self._script is None:
            self._script = self.redis.register_script(RATE_LIMIT_LUA)
        return self._script

    async def check(self, client_id: str) -> RateLimitResult:
        """Check if a request from client_id is allowed.

        Args:
            client_id: Unique identifier for the client (IP or API key).

        Returns:
            RateLimitResult with allowed status and metadata.
        """
        now = time.time()
        window_start = now - self.window_seconds
        member = f"{now}:{uuid.uuid4().hex[:8]}"
        ttl = self.window_seconds + 1
        key = f"ratelimit:{client_id}"

        script = await self._get_script()
        result = await script(
            keys=[key],
            args=[str(window_start), str(now), str(self.max_requests), member, str(ttl)],
        )

        allowed = bool(result[0])
        remaining = int(result[1])
        reset_at = float(result[2])
        retry_after_val = float(result[3])

        return RateLimitResult(
            allowed=allowed,
            limit=self.max_requests,
            remaining=remaining,
            reset_at=reset_at,
            retry_after=retry_after_val if not allowed else None,
        )

    async def get_usage(self, client_id: str) -> dict:
        """Get current usage info for a client without consuming a request."""
        now = time.time()
        window_start = now - self.window_seconds
        key = f"ratelimit:{client_id}"

        pipe = self.redis.pipeline()
        pipe.zremrangebyscore(key, 0, window_start)
        pipe.zcard(key)
        results = await pipe.execute()

        current_count = results[1]
        return {
            "client_id": client_id,
            "current_count": current_count,
            "limit": self.max_requests,
            "remaining": max(0, self.max_requests - current_count),
            "window_seconds": self.window_seconds,
        }
