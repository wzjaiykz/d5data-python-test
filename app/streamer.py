"""Token generator that imitates an LLM stream.

The demo deliberately avoids a real model: what the task asks for is the
*transport* (chunked / SSE), not the model behind it.
"""

import asyncio
import random
from collections.abc import AsyncIterator

WORDS = (
    "Streaming responses let a client render tokens as they arrive, "
    "instead of waiting for the whole answer. This demo simulates a model "
    "and sends one word at a time over a chunked HTTP response."
).split()


async def stream_words(prompt: str, delay: float = 0.05) -> AsyncIterator[str]:
    """Yield one word at a time with a small delay, like a token stream."""
    yield f"echo: {prompt.strip()}\n\n" if prompt.strip() else "echo: (empty)\n\n"
    for word in WORDS:
        await asyncio.sleep(delay + random.random() * delay)  # noqa: S311 - demo jitter
        yield word + " "
    yield "\n\n[done]"
