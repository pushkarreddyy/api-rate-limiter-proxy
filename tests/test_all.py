"""Self-contained test runner and verification suite.

Can be run directly via: python tests/test_all.py

Author: @pushkarreddyy
Repository: https://github.com/pushkarreddyy/api-rate-limiter-proxy
"""

import asyncio
import time
import uuid
import httpx
from fastapi.responses import JSONResponse

from src.rate_limiter import SlidingWindowRateLimiter, RateLimitResult
from src.analytics import AnalyticsService
from src.proxy import _sanitize_headers, HOP_BY_HOP_HEADERS
from mock_backend.server import app as mock_backend_app


class SimpleInMemoryRedis:
    """In-memory Redis simulator for sorted sets, hashes, lists, and counters."""

    def __init__(self):
        self.zsets = {}      # key -> [(score, member), ...]
        self.hashes = {}     # key -> {field: str(val)}
        self.lists = {}      # key -> [item, ...]
        self.strings = {}    # key -> str(val)

    def pipeline(self):
        return SimpleInMemoryPipeline(self)

    async def ping(self):
        return True

    def register_script(self, script_str):
        # Return a callable that executes the sliding window logic atomically
        async def runner(keys, args):
            key = keys[0]
            window_start = float(args[0])
            now = float(args[1])
            max_requests = int(args[2])
            member = args[3]
            ttl = float(args[4])

            # 1. Prune expired entries
            zset = self.zsets.get(key, [])
            zset = [(s, m) for s, m in zset if s > window_start]

            current_count = len(zset)
            if current_count < max_requests:
                zset.append((now, member))
                self.zsets[key] = zset
                remaining = max_requests - current_count - 1
                reset_at = now + (ttl - 1)
                return [1, remaining, str(reset_at), "0"]
            else:
                self.zsets[key] = zset
                oldest_score = min(s for s, m in zset) if zset else now
                retry_after = max(1.0, oldest_score + (ttl - 1) - now)
                reset_at = now + retry_after
                return [0, 0, str(reset_at), str(retry_after)]

        return runner

    async def zcount(self, key, min_score, max_score):
        zset = self.zsets.get(key, [])
        return sum(1 for s, m in zset if min_score <= s <= max_score)

    async def hgetall(self, key):
        return self.hashes.get(key, {})

    async def zrevrange(self, key, start, end, withscores=False):
        zset = sorted(self.zsets.get(key, []), key=lambda x: x[0], reverse=True)
        slice_items = zset[start : end + 1] if end >= 0 else zset[start:]
        if withscores:
            return [(m, s) for s, m in slice_items]
        return [m for s, m in slice_items]

    async def lrange(self, key, start, end):
        lst = self.lists.get(key, [])
        return lst[start : end + 1] if end >= 0 else lst[start:]

    async def aclose(self):
        pass


class SimpleInMemoryPipeline:
    def __init__(self, db):
        self.db = db
        self.ops = []

    def zadd(self, key, mapping):
        for member, score in mapping.items():
            self.ops.append(('zadd', key, score, member))
        return self

    def zremrangebyscore(self, key, min_s, max_s):
        self.ops.append(('zremrangebyscore', key, min_s, max_s))
        return self

    def hincrby(self, key, field, amount=1):
        self.ops.append(('hincrby', key, field, amount))
        return self

    def zincrby(self, key, amount, member):
        self.ops.append(('zincrby', key, amount, member))
        return self

    def lpush(self, key, value):
        self.ops.append(('lpush', key, value))
        return self

    def ltrim(self, key, start, end):
        self.ops.append(('ltrim', key, start, end))
        return self

    def incr(self, key):
        self.ops.append(('incr', key))
        return self

    def incrbyfloat(self, key, amount):
        self.ops.append(('incrbyfloat', key, amount))
        return self

    def get(self, key):
        self.ops.append(('get', key))
        return self

    def zcount(self, key, min_s, max_s):
        self.ops.append(('zcount', key, min_s, max_s))
        return self

    def zcard(self, key):
        self.ops.append(('zcard', key))
        return self

    async def execute(self):
        results = []
        for op in self.ops:
            name = op[0]
            if name == 'zadd':
                _, key, score, member = op
                lst = self.db.zsets.setdefault(key, [])
                lst.append((score, member))
                results.append(1)
            elif name == 'zremrangebyscore':
                _, key, min_s, max_s = op
                lst = self.db.zsets.get(key, [])
                before = len(lst)
                self.db.zsets[key] = [(s, m) for s, m in lst if not (min_s <= s <= max_s)]
                results.append(before - len(self.db.zsets[key]))
            elif name == 'hincrby':
                _, key, field, amount = op
                h = self.db.hashes.setdefault(key, {})
                cur = int(h.get(field, 0)) + amount
                h[field] = str(cur)
                results.append(cur)
            elif name == 'zincrby':
                _, key, amount, member = op
                lst = self.db.zsets.setdefault(key, [])
                new_score = amount
                for idx, (s, m) in enumerate(lst):
                    if m == member:
                        new_score = s + amount
                        lst[idx] = (new_score, member)
                        break
                else:
                    lst.append((new_score, member))
                results.append(new_score)
            elif name == 'lpush':
                _, key, value = op
                lst = self.db.lists.setdefault(key, [])
                lst.insert(0, value)
                results.append(len(lst))
            elif name == 'ltrim':
                _, key, start, end = op
                lst = self.db.lists.get(key, [])
                self.db.lists[key] = lst[start : end + 1]
                results.append(True)
            elif name == 'incr':
                _, key = op
                cur = int(self.db.strings.get(key, 0)) + 1
                self.db.strings[key] = str(cur)
                results.append(cur)
            elif name == 'incrbyfloat':
                _, key, amount = op
                cur = float(self.db.strings.get(key, 0.0)) + float(amount)
                self.db.strings[key] = str(cur)
                results.append(cur)
            elif name == 'get':
                _, key = op
                results.append(self.db.strings.get(key, None))
            elif name == 'zcount':
                _, key, min_s, max_s = op
                lst = self.db.zsets.get(key, [])
                results.append(sum(1 for s, m in lst if min_s <= s <= max_s))
            elif name == 'zcard':
                _, key = op
                results.append(len(self.db.zsets.get(key, [])))
        return results


async def run_all_tests():
    print("==================================================")
    print("[TEST SUITE] Running API Rate Limiter & Reverse Proxy Tests")
    print("==================================================")
    passed = 0
    total = 0

    # 1. Test Hop-by-Hop Sanitization
    total += 1
    raw_headers = [
        (b"host", b"localhost:8000"),
        (b"connection", b"keep-alive"),
        (b"transfer-encoding", b"chunked"),
        (b"user-agent", b"curl/8.0"),
        (b"content-type", b"application/json"),
    ]
    sanitized = _sanitize_headers(raw_headers)
    assert "connection" not in sanitized
    assert "transfer-encoding" not in sanitized
    assert "host" not in sanitized
    assert sanitized["user-agent"] == "curl/8.0"
    assert sanitized["content-type"] == "application/json"
    print("[PASS] Test 1: RFC 9110 Hop-by-Hop Header Sanitization")
    passed += 1

    # 2. Test Sliding Window Rate Limiting (Normal Quota)
    total += 1
    db = SimpleInMemoryRedis()
    limiter = SlidingWindowRateLimiter(db, max_requests=5, window_seconds=10)
    for i in range(5):
        res = await limiter.check("client_1")
        assert res.allowed is True
        assert res.remaining == 4 - i
        assert res.limit == 5
    print("[PASS] Test 2: Sliding Window Allowed Requests & Decreasing Quota")
    passed += 1

    # 3. Test 429 Too Many Requests Blocking & Retry-After
    total += 1
    blocked = await limiter.check("client_1")
    assert blocked.allowed is False
    assert blocked.remaining == 0
    assert blocked.retry_after is not None
    assert blocked.retry_after > 0
    print(f"[PASS] Test 3: 429 Rate Limit Exceeded (Retry-After: {blocked.retry_after:.1f}s)")
    passed += 1

    # 4. Test Client Quota Isolation
    total += 1
    other_client = await limiter.check("client_2")
    assert other_client.allowed is True
    assert other_client.remaining == 4
    print("[PASS] Test 4: Isolated Client Quotas (Client 2 unaffected by Client 1)")
    passed += 1

    # 5. Test Analytics Service
    total += 1
    analytics = AnalyticsService(db)
    await analytics.record("client_1", "GET", "/api/v1/users", 200, 12.5)
    await analytics.record("client_1", "GET", "/api/v1/users", 200, 15.0)
    await analytics.record("client_2", "GET", "/api/v1/users", 429, 1.2)
    await analytics.record("client_3", "GET", "/api/v1/error", 500, 30.0)

    summary = await analytics.get_summary()
    assert summary["total_requests"] == 4
    assert summary["unique_consumers"] == 3
    assert summary["avg_latency_ms"] > 0

    errors = await analytics.get_error_breakdown()
    assert errors["2xx"] == 2
    assert errors["4xx"] == 1
    assert errors["5xx"] == 1

    top = await analytics.get_top_consumers(limit=5)
    assert top[0]["client_id"] == "client_1"
    assert top[0]["request_count"] == 2
    print("[PASS] Test 5: Metrics & Analytics Aggregation (RPM, Status Codes, Top Consumers)")
    passed += 1

    # 6. Test Mock Backend Endpoints (Users, Slow, Stream)
    total += 1
    transport = httpx.ASGITransport(app=mock_backend_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://mock-backend") as client:
        # GET users
        resp = await client.get("/api/v1/users")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["users"]) == 5

        # POST user
        post_resp = await client.post("/api/v1/users", json={"name": "Test User", "role": "tester"})
        assert post_resp.status_code == 201
        assert post_resp.json()["name"] == "Test User"

        # GET stream
        stream_chunks = []
        async with client.stream("GET", "/api/v1/stream") as stream_resp:
            assert stream_resp.status_code == 200
            async for chunk in stream_resp.aiter_lines():
                if chunk:
                    stream_chunks.append(chunk)
        assert len(stream_chunks) == 10
    print("[PASS] Test 6: Mock Backend Endpoints (CRUD + Streaming Verification)")
    passed += 1

    print("==================================================")
    print(f"[SUCCESS] ALL {passed}/{total} TESTS PASSED SUCCESSFULLY!")
    print("==================================================")


if __name__ == "__main__":
    asyncio.run(run_all_tests())
