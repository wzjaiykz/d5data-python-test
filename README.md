# d5data-python-test — FastAPI streaming demo

A small FastAPI service that streams a response token by token, the way an LLM
chat API does. Three endpoints, one browser page, six tests.

## Run

```bash
python -m venv .venv && .venv\Scripts\activate     # Windows
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Then open <http://127.0.0.1:8000/> for a browser client, or use curl:

```bash
# plain chunked stream: output appears word by word
curl -N "http://127.0.0.1:8000/api/stream?prompt=hello"

# Server-Sent Events: each frame is JSON with an index
curl -N "http://127.0.0.1:8000/api/events?prompt=hello"

# heartbeat, useful for smoke tests
curl -N "http://127.0.0.1:8000/api/ticks?count=3&interval=0.2"
```

`-N` disables curl's buffering; without it you see the whole body at once and
might think nothing is streaming.

## Endpoints

| Method | Path | Behaviour |
|---|---|---|
| GET | `/` | Minimal HTML client using `fetch` + `ReadableStream` |
| GET | `/api/stream?prompt=` | `text/plain` chunked response, one word per chunk |
| GET | `/api/events?prompt=` | `text/event-stream`, `data: {"index":..,"delta":..}` |
| GET | `/api/ticks?count=&interval=` | Timed heartbeat lines |

## What makes it streaming

- The handler returns a `StreamingResponse` wrapping an async generator; nothing
  is buffered before the first byte leaves the server.
- `media_type="text/plain"` for chunks, `text/event-stream` for SSE.
- `Cache-Control: no-store` and `X-Accel-Buffering: no` — the second one stops
  nginx from buffering the stream, which is the classic "works locally, batches
  behind a proxy" bug.
- A length limit (`max_length=200`) keeps a caller from pushing an unbounded
  prompt through the generator.

## Tests

```bash
pytest -q
```

Six tests: the generator emits many chunks; the chunked endpoint returns the
whole answer; SSE frames are ordered and valid JSON; `count` is respected; an
over-long prompt is rejected with 422; a client can stop reading early.

One subtlety worth knowing: HTTPX's `ASGITransport` may coalesce chunks, so
"is it really streaming?" is asserted on the generator (`test_generator_really_streams`)
instead of on client chunk boundaries — an assertion on the client would be
testing the test client, not the server.

## Notes

- The "model" is a canned sentence in `app/streamer.py`; the task is about the
  transport, so no API key is required.
- `--reload` is for development only.
