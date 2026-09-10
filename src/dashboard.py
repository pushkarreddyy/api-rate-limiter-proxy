"""Dashboard API routes and static file serving.

Provides JSON endpoints for the analytics dashboard and serves
the static HTML dashboard page.
"""

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, FileResponse

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


@router.get("/", response_class=HTMLResponse)
async def dashboard_page() -> FileResponse:
    """Serve the analytics dashboard HTML page."""
    return FileResponse(STATIC_DIR / "dashboard.html", media_type="text/html")


@router.get("/api/rpm")
async def rpm_series(request: Request) -> list[dict]:
    """Requests per minute for the last 60 minutes."""
    analytics = request.app.state.analytics
    return await analytics.get_rpm_series(minutes=60)


@router.get("/api/errors")
async def error_breakdown(request: Request) -> dict:
    """Error rate breakdown by status code family (2xx, 3xx, 4xx, 5xx)."""
    analytics = request.app.state.analytics
    return await analytics.get_error_breakdown()


@router.get("/api/top-consumers")
async def top_consumers(request: Request) -> list[dict]:
    """Top 10 consumers by request count."""
    analytics = request.app.state.analytics
    return await analytics.get_top_consumers(limit=10)


@router.get("/api/recent")
async def recent_requests(request: Request) -> list[dict]:
    """Last 100 proxied requests with timing data."""
    analytics = request.app.state.analytics
    return await analytics.get_recent_requests(limit=100)


@router.get("/api/summary")
async def summary_stats(request: Request) -> dict:
    """High-level summary: total requests, avg latency, current RPM, unique consumers."""
    analytics = request.app.state.analytics
    return await analytics.get_summary()
