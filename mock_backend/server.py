"""Mock upstream backend API for testing the reverse proxy.

Run directly: python -m mock_backend.server
Serves on port 9000.
"""

import asyncio
import json
from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI(title="Mock Backend API", version="1.0.0")

# --- Mock data ---
MOCK_USERS = [
    {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "role": "admin"},
    {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "role": "user"},
    {"id": 3, "name": "Charlie Brown", "email": "charlie@example.com", "role": "user"},
    {"id": 4, "name": "Diana Prince", "email": "diana@example.com", "role": "moderator"},
    {"id": 5, "name": "Eve Wilson", "email": "eve@example.com", "role": "user"},
]


@app.get("/api/v1/users")
async def list_users():
    """Return all mock users."""
    return {"users": MOCK_USERS, "count": len(MOCK_USERS)}


@app.get("/api/v1/users/{user_id}")
async def get_user(user_id: int):
    """Return a single user by ID."""
    for user in MOCK_USERS:
        if user["id"] == user_id:
            return user
    return JSONResponse(status_code=404, content={"error": "User not found"})


@app.post("/api/v1/users")
async def create_user(request: Request):
    """Echo the posted body back as a created user."""
    body = await request.json()
    new_user = {"id": len(MOCK_USERS) + 1, **body}
    return JSONResponse(status_code=201, content=new_user)


@app.get("/api/v1/slow")
async def slow_endpoint():
    """Simulate a slow response (2 second delay)."""
    await asyncio.sleep(2)
    return {"message": "This response was delayed by 2 seconds", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/v1/stream")
async def stream_endpoint():
    """Return a streaming response to test stream forwarding."""
    async def generate():
        for i in range(10):
            chunk = json.dumps({"chunk": i, "data": f"Streaming chunk {i}"}) + "\n"
            yield chunk
            await asyncio.sleep(0.3)
    return StreamingResponse(generate(), media_type="application/x-ndjson")


@app.get("/api/v1/error")
async def error_endpoint():
    """Return a 500 error to test error forwarding."""
    return JSONResponse(
        status_code=500,
        content={"error": "Internal Server Error", "message": "Something went wrong on the upstream server."},
    )


@app.get("/api/v1/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "mock-backend", "timestamp": datetime.now(timezone.utc).isoformat()}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9000)
