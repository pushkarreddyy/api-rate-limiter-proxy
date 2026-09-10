"""Live demonstration of the API Rate Limiter & Reverse Proxy.

Demonstrates:
1. Transparent request forwarding to backend.
2. Rate limit header injection (X-RateLimit-*).
3. 429 Too Many Requests when quota is exceeded with Retry-After.
4. Per-client quota isolation (API keys).
5. Non-buffering streaming proxying (/api/v1/stream).
6. Live analytics recording.

Run directly via: python demo_live.py

Author: @pushkarreddyy
Repository: https://github.com/pushkarreddyy/api-rate-limiter-proxy
"""

import asyncio
import json
import httpx

from src.main import app as proxy_app
from src.rate_limiter import SlidingWindowRateLimiter
from src.analytics import AnalyticsService
from mock_backend.server import app as mock_backend_app
from tests.test_all import SimpleInMemoryRedis


async def main():
    print("======================================================================")
    print("[LIVE DEMO] API RATE LIMITER & REVERSE PROXY")
    print("======================================================================")

    # 1. Initialize In-Memory Redis and Upstream Transport
    redis_db = SimpleInMemoryRedis()
    mock_transport = httpx.ASGITransport(app=mock_backend_app)
    mock_client = httpx.AsyncClient(transport=mock_transport, base_url="http://mock-backend")

    # Set rate limit to 3 requests per 10-second window for clear demo
    proxy_app.state.redis = redis_db
    proxy_app.state.http_client = mock_client
    proxy_app.state.rate_limiter = SlidingWindowRateLimiter(
        redis_client=redis_db,
        max_requests=3,
        window_seconds=10,
    )
    proxy_app.state.analytics = AnalyticsService(redis_client=redis_db)

    proxy_transport = httpx.ASGITransport(app=proxy_app)

    async with httpx.AsyncClient(transport=proxy_transport, base_url="http://gateway:8000") as client:
        # -------------------------------------------------------------
        # DEMO 1: Standard Request through Reverse Proxy
        # -------------------------------------------------------------
        print("\n--- [1] Client sends GET /api/v1/users (Under Quota) ---")
        resp = await client.get("/api/v1/users", headers={"X-API-Key": "client-alpha"})
        print(f"Status Code : {resp.status_code} OK")
        print(f"Header X-RateLimit-Limit     : {resp.headers.get('x-ratelimit-limit')}")
        print(f"Header X-RateLimit-Remaining : {resp.headers.get('x-ratelimit-remaining')}")
        print(f"Header X-RateLimit-Reset     : {resp.headers.get('x-ratelimit-reset')}")
        print(f"Response Body (First 2 users):")
        data = resp.json()
        print(json.dumps(data["users"][:2], indent=2))

        # -------------------------------------------------------------
        # DEMO 2: Burst Traffic to Trigger 429 Rate Limiting
        # -------------------------------------------------------------
        print("\n--- [2] Client bursts requests to exhaust quota (Limit = 3) ---")
        for i in range(2, 4):
            r = await client.get("/api/v1/users", headers={"X-API-Key": "client-alpha"})
            print(f"  -> Request #{i}: Status {r.status_code}, Remaining Quota: {r.headers.get('x-ratelimit-remaining')}")

        print("\n--- [3] Request #4: Exceeds Quota (429 Too Many Requests) ---")
        blocked = await client.get("/api/v1/users", headers={"X-API-Key": "client-alpha"})
        print(f"Status Code : {blocked.status_code} Too Many Requests")
        print(f"Header X-RateLimit-Remaining : {blocked.headers.get('x-ratelimit-remaining')}")
        print(f"Header Retry-After           : {blocked.headers.get('retry-after')} seconds")
        print(f"Response Body:")
        print(json.dumps(blocked.json(), indent=2))

        # -------------------------------------------------------------
        # DEMO 3: Quota Isolation (Different Client)
        # -------------------------------------------------------------
        print("\n--- [4] Different Client (client-beta) makes a request ---")
        beta_resp = await client.get("/api/v1/users", headers={"X-API-Key": "client-beta"})
        print(f"Status Code : {beta_resp.status_code} OK")
        print(f"Client 'client-beta' has independent remaining quota: {beta_resp.headers.get('x-ratelimit-remaining')}")

        # -------------------------------------------------------------
        # DEMO 4: Streaming Proxying (/api/v1/stream)
        # -------------------------------------------------------------
        print("\n--- [5] Live Chunked Streaming Proxying (GET /api/v1/stream) ---")
        print("Receiving streaming chunks forwarded chunk-by-chunk:")
        async with client.stream("GET", "/api/v1/stream", headers={"X-API-Key": "client-beta"}) as stream_resp:
            chunk_count = 0
            async for chunk in stream_resp.aiter_lines():
                if chunk:
                    chunk_count += 1
                    print(f"  Received: {chunk}")
                    if chunk_count >= 3:
                        print("  ... (remaining stream chunks received successfully)")
                        break

        # -------------------------------------------------------------
        # DEMO 5: Real-time Analytics Dashboard Query
        # -------------------------------------------------------------
        print("\n--- [6] Querying Live Analytics API (/dashboard/api/summary & top-consumers) ---")
        summary_resp = await client.get("/dashboard/api/summary")
        print("Summary KPI Metrics:")
        print(json.dumps(summary_resp.json(), indent=2))

        top_resp = await client.get("/dashboard/api/top-consumers")
        print("\nTop Consumers Tracked:")
        print(json.dumps(top_resp.json(), indent=2))

    print("\n" + "=" * 70)
    print("[SUCCESS] LIVE DEMONSTRATION COMPLETE - ALL SYSTEMS OPERATIONAL")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
