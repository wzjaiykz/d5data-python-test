"""FastAPI streaming demo: chunked text, SSE and a cancel-friendly endpoint."""

import asyncio
import json
import time
from collections.abc import AsyncIterator

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, StreamingResponse

from app.streamer import stream_words

app = FastAPI(title="d5data-python-test", version="1.0.0")

INDEX = """<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>Streaming demo</title></head>
<body style="font-family: system-ui; max-width: 720px; margin: 40px auto">
  <h1>FastAPI streaming demo</h1>
  <p><button id="start">Start stream</button> <button id="stop">Stop</button></p>
  <pre id="out" style="background:#f5f5f5;padding:16px;min-height:120px"></pre>
  <script>
    let controller;
    document.getElementById('start').onclick = async () => {
      const out = document.getElementById('out');
      out.textContent = '';
      controller = new AbortController();
      const response = await fetch('/api/stream?prompt=hello', {signal: controller.signal});
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      while (true) {
        const {value, done} = await reader.read();
        if (done) break;
        out.textContent += decoder.decode(value);
      }
    };
    document.getElementById('stop').onclick = () => controller && controller.abort();
  </script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    """A tiny browser client, so the demo can be shown without curl."""
    return INDEX


@app.get("/api/stream")
async def stream(prompt: str = Query("", max_length=200)) -> StreamingResponse:
    """Plain chunked response: every word arrives as its own chunk."""
    return StreamingResponse(
        stream_words(prompt),
        media_type="text/plain; charset=utf-8",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )


@app.get("/api/events")
async def events(prompt: str = Query("", max_length=200)) -> StreamingResponse:
    """Server-Sent Events: each chunk is a JSON payload with an index."""
    async def generator() -> AsyncIterator[str]:
        index = 0
        async for word in stream_words(prompt):
            payload = json.dumps({"index": index, "delta": word}, ensure_ascii=False)
            yield f"data: {payload}\n\n"
            index += 1
            await asyncio.sleep(0)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )


@app.get("/api/ticks")
async def ticks(count: int = Query(10, ge=1, le=100), interval: float = Query(0.1, gt=0)) -> StreamingResponse:
    """Minimal heartbeat stream; handy for curl demos and tests."""
    async def generator() -> AsyncIterator[str]:
        for index in range(count):
            yield f"{time.time():.3f} tick {index}\n"
            await asyncio.sleep(interval)

    return StreamingResponse(
        generator(),
        media_type="text/plain; charset=utf-8",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )
