"""Tests for the streaming endpoints.

Note: HTTPX's ASGI transport may coalesce chunks, so "is it really streaming?"
is asserted at the generator level, not on client chunk boundaries.
"""

import json

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.streamer import stream_words


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client


async def test_generator_really_streams():
    chunks = [chunk async for chunk in stream_words("hi", delay=0)]
    assert len(chunks) > 3
    assert chunks[0].startswith("echo: hi")


async def test_stream_endpoint_returns_the_whole_answer(client):
    async with client.stream("GET", "/api/stream", params={"prompt": "hi"}) as response:
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/plain")
        body = "".join([chunk async for chunk in response.aiter_text()])

    assert "echo: hi" in body
    assert body.rstrip().endswith("[done]")


async def test_events_returns_sse_frames(client):
    frames = []
    async with client.stream("GET", "/api/events", params={"prompt": "sse"}) as response:
        assert response.headers["content-type"].startswith("text/event-stream")
        async for line in response.aiter_lines():
            if line.startswith("data: "):
                frames.append(json.loads(line[len("data: ") :]))

    assert len(frames) >= 2
    assert [frame["index"] for frame in frames] == list(range(len(frames)))
    assert "echo: sse" in frames[0]["delta"]


async def test_ticks_respects_count(client):
    async with client.stream("GET", "/api/ticks", params={"count": 3, "interval": 0.01}) as response:
        lines = [line async for line in response.aiter_lines() if line.strip()]

    assert len(lines) == 3


async def test_prompt_is_length_limited(client):
    response = await client.get("/api/stream", params={"prompt": "x" * 500})
    assert response.status_code == 422


async def test_client_can_stop_early(client):
    received = 0
    async with client.stream("GET", "/api/ticks", params={"count": 50, "interval": 0.01}) as response:
        async for _ in response.aiter_lines():
            received += 1
            if received == 3:
                break

    assert received == 3
