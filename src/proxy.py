"""Reverse proxy handler.

Forwards incoming requests to the upstream backend service using httpx.
Streams both request and response bodies to avoid buffering large payloads
in memory. Sanitizes hop-by-hop headers per RFC 9110.

Author: Pushkar Reddy (@pushkarreddyy)
Repository: https://github.com/pushkarreddyy/api-rate-limiter-proxy
"""

import httpx
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from starlette.background import BackgroundTask

router = APIRouter()

# Headers that must NOT be forwarded between hops (RFC 9110 / RFC 7230).
HOP_BY_HOP_HEADERS = frozenset({
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
})


def _sanitize_headers(
    raw_headers: list[tuple[bytes, bytes]],
    *,
    strip_content_length: bool = False,
) -> dict[str, str]:
    """Filter hop-by-hop headers from a raw header list."""
    out: dict[str, str] = {}
    for name_bytes, value_bytes in raw_headers:
        name = name_bytes.decode("latin-1").lower()
        if name in HOP_BY_HOP_HEADERS:
            continue
        if strip_content_length and name == "content-length":
            continue
        out[name] = value_bytes.decode("latin-1")
    return out


@router.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
)
async def reverse_proxy(request: Request, path: str) -> StreamingResponse:
    """Forward the request to the upstream service and stream the response back.

    This handler:
    1. Sanitises hop-by-hop headers from the incoming request.
    2. Adds X-Forwarded-For / X-Forwarded-Proto headers.
    3. Streams the request body without buffering (supports large uploads).
    4. Streams the upstream response back to the client.
    5. Closes the upstream connection in a background task to prevent leaks.
    """
    http_client: httpx.AsyncClient = request.app.state.http_client

    # --- Build forwarding headers ---
    upstream_headers = _sanitize_headers(request.headers.raw)
    client_ip = request.headers.get(
        "X-Forwarded-For", request.client.host if request.client else "127.0.0.1"
    ).split(",")[0].strip()
    upstream_headers["x-forwarded-for"] = client_ip
    upstream_headers["x-forwarded-proto"] = request.url.scheme

    # --- Build the upstream URL ---
    upstream_url = httpx.URL(
        path=f"/{path}",
        query=request.url.query.encode("utf-8") if request.url.query else None,
    )

    # --- Build and send the request, streaming the body ---
    try:
        rp_req = http_client.build_request(
            method=request.method,
            url=upstream_url,
            headers=upstream_headers,
            content=request.stream(),
        )
        upstream_resp = await http_client.send(rp_req, stream=True)
    except httpx.TimeoutException:
        return JSONResponse(
            status_code=504,
            content={
                "error": "gateway_timeout",
                "message": "The upstream service did not respond in time.",
            },
        )
    except httpx.RequestError as exc:
        return JSONResponse(
            status_code=502,
            content={
                "error": "bad_gateway",
                "message": f"Could not reach upstream service: {exc}",
            },
        )

    # --- Stream the response back, stripping hop-by-hop headers ---
    response_headers = _sanitize_headers(
        upstream_resp.headers.raw,
        strip_content_length=True,
    )
    # Disable buffering by intermediate proxies (e.g. Nginx)
    response_headers["x-accel-buffering"] = "no"

    # Store status code on request state so middleware can read it
    request.state.upstream_status = upstream_resp.status_code

    return StreamingResponse(
        upstream_resp.aiter_raw(),
        status_code=upstream_resp.status_code,
        headers=response_headers,
        background=BackgroundTask(upstream_resp.aclose),
    )
