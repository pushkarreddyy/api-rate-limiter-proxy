"""Analytics service for recording and querying proxy metrics.

Uses Redis data structures:
- Sorted Set for RPM time series (score=timestamp, member=request_id)
- Hash for status code counts
- Sorted Set for top consumer tracking
- List for recent request log

Author: @pushkarreddyy
Repository: https://github.com/pushkarreddyy/api-rate-limiter-proxy
"""

import json
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

import redis.asyncio as redis


# Redis key prefixes
KEY_RPM = "analytics:rpm"
KEY_STATUS = "analytics:status_codes"
KEY_CONSUMERS = "analytics:consumers"
KEY_RECENT = "analytics:recent"
KEY_TOTAL = "analytics:total_requests"
KEY_LATENCY_SUM = "analytics:latency_sum"
KEY_LATENCY_COUNT = "analytics:latency_count"


@dataclass
class RequestLog:
    """A single request log entry."""

    request_id: str
    timestamp: float
    client_id: str
    method: str
    path: str
    status_code: int
    latency_ms: float


class AnalyticsService:
    """Records and queries proxy request analytics."""

    def __init__(self, redis_client: redis.Redis) -> None:
        self.redis = redis_client

    async def record(
        self,
        client_id: str,
        method: str,
        path: str,
        status_code: int,
        latency_ms: float,
    ) -> None:
        """Record a completed proxy request."""
        now = time.time()
        request_id = uuid.uuid4().hex[:12]

        log_entry = RequestLog(
            request_id=request_id,
            timestamp=now,
            client_id=client_id,
            method=method,
            path=path,
            status_code=status_code,
            latency_ms=round(latency_ms, 2),
        )

        pipe = self.redis.pipeline()

        # 1. RPM tracking — sorted set with timestamp score
        pipe.zadd(KEY_RPM, {f"{request_id}:{now}": now})
        # Prune entries older than 1 hour
        pipe.zremrangebyscore(KEY_RPM, 0, now - 3600)

        # 2. Status code counts
        pipe.hincrby(KEY_STATUS, str(status_code), 1)

        # 3. Top consumers — increment score for this client
        pipe.zincrby(KEY_CONSUMERS, 1, client_id)

        # 4. Recent requests log (keep last 200)
        pipe.lpush(KEY_RECENT, json.dumps(asdict(log_entry)))
        pipe.ltrim(KEY_RECENT, 0, 199)

        # 5. Global counters
        pipe.incr(KEY_TOTAL)
        pipe.incrbyfloat(KEY_LATENCY_SUM, latency_ms)
        pipe.incr(KEY_LATENCY_COUNT)

        await pipe.execute()

    async def get_rpm_series(self, minutes: int = 60) -> list[dict]:
        """Get requests-per-minute for the last N minutes."""
        now = time.time()
        result = []

        for i in range(minutes - 1, -1, -1):
            minute_start = now - (i + 1) * 60
            minute_end = now - i * 60
            count = await self.redis.zcount(KEY_RPM, minute_start, minute_end)
            timestamp = datetime.fromtimestamp(minute_end, tz=timezone.utc).strftime("%H:%M")
            result.append({"time": timestamp, "count": count})

        return result

    async def get_error_breakdown(self) -> dict:
        """Get request counts grouped by status code family."""
        raw = await self.redis.hgetall(KEY_STATUS)
        families = {"2xx": 0, "3xx": 0, "4xx": 0, "5xx": 0, "other": 0}

        for code_str, count_str in raw.items():
            code = int(code_str)
            count = int(count_str)
            if 200 <= code < 300:
                families["2xx"] += count
            elif 300 <= code < 400:
                families["3xx"] += count
            elif 400 <= code < 500:
                families["4xx"] += count
            elif 500 <= code < 600:
                families["5xx"] += count
            else:
                families["other"] += count

        return families

    async def get_top_consumers(self, limit: int = 10) -> list[dict]:
        """Get top N consumers by request count."""
        # ZREVRANGE with scores returns [(member, score), ...]
        results = await self.redis.zrevrange(KEY_CONSUMERS, 0, limit - 1, withscores=True)
        return [
            {"client_id": member, "request_count": int(score)}
            for member, score in results
        ]

    async def get_recent_requests(self, limit: int = 100) -> list[dict]:
        """Get the most recent N request logs."""
        raw = await self.redis.lrange(KEY_RECENT, 0, limit - 1)
        return [json.loads(entry) for entry in raw]

    async def get_summary(self) -> dict:
        """Get high-level summary stats."""
        now = time.time()

        pipe = self.redis.pipeline()
        pipe.get(KEY_TOTAL)
        pipe.get(KEY_LATENCY_SUM)
        pipe.get(KEY_LATENCY_COUNT)
        pipe.zcount(KEY_RPM, now - 60, now)  # Current RPM
        pipe.zcard(KEY_CONSUMERS)  # Unique consumers
        results = await pipe.execute()

        total = int(results[0] or 0)
        latency_sum = float(results[1] or 0)
        latency_count = int(results[2] or 0)
        current_rpm = int(results[3] or 0)
        unique_consumers = int(results[4] or 0)

        avg_latency = round(latency_sum / latency_count, 2) if latency_count > 0 else 0.0

        return {
            "total_requests": total,
            "avg_latency_ms": avg_latency,
            "current_rpm": current_rpm,
            "unique_consumers": unique_consumers,
        }
