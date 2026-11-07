#!/usr/bin/env python3
import re
import json
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

app = FastAPI(title="LLM Streaming Guardrail")

UPSTREAM_LLM_URL = "http://127.0.0.1:9000/v1/chat/completions"

# Compiled regex patterns for PII
EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
SSN_REGEX = r"\b\d{3}-\d{2}-\d{4}\b"
CCN_REGEX = r"\b(?:\d{4}[ -]?){3}\d{4}\b"
PII_PATTERN = re.compile(f"({EMAIL_REGEX}|{SSN_REGEX}|{CCN_REGEX})")

# Lookahead buffer size handles patterns split across adjacent SSE chunk deltas
LOOKAHEAD_SAFE_BUFFER = 64

class StreamRedactor:
    def __init__(self):
        self.buffer = ""

    def process_text_delta(self, delta: str) -> str:
        self.buffer += delta
        # If buffer is shorter than the lookahead window, hold to ensure boundary matches
        if len(self.buffer) <= LOOKAHEAD_SAFE_BUFFER:
            return ""

        # Safe region can be scanned and released
        flush_idx = len(self.buffer) - LOOKAHEAD_SAFE_BUFFER
        safe_to_scan = self.buffer[:flush_idx]
        self.buffer = self.buffer[flush_idx:]

        return PII_PATTERN.sub("[REDACTED]", safe_to_scan)

    def flush(self) -> str:
        final_text = self.buffer
        self.buffer = ""
        return PII_PATTERN.sub("[REDACTED]", final_text)

async def stream_processor(upstream_response: httpx.Response):
    redactor = StreamRedactor()

    async for raw_line in upstream_response.aiter_lines():
        if not raw_line.startswith("data: "):
            if raw_line:
                yield f"{raw_line}\n"
            continue

        raw_payload = raw_line[6:].strip()
        if raw_payload == "[DONE]":
            remaining = redactor.flush()
            if remaining:
                chunk_obj = {
                    "choices": [{"delta": {"content": remaining}, "index": 0}]
                }
                yield f"data: {json.dumps(chunk_obj)}\n\n"
            yield "data: [DONE]\n\n"
            break

        try:
            chunk = json.loads(raw_payload)
            content_delta = chunk.get("choices", [{}])[0].get("delta", {}).get("content", "")
            if content_delta:
                sanitized = redactor.process_text_delta(content_delta)
                if sanitized:
                    chunk["choices"][0]["delta"]["content"] = sanitized
                    yield f"data: {json.dumps(chunk)}\n\n"
            else:
                yield f"{raw_line}\n\n"
        except json.JSONDecodeError:
            yield f"{raw_line}\n\n"

@app.post("/v1/chat/completions")
async def chat_completions_proxy(request: Request):
    req_body = await request.json()
    req_body["stream"] = True

    client = httpx.AsyncClient(timeout=60.0)
    upstream_req = client.build_request("POST", UPSTREAM_LLM_URL, json=req_body)
    upstream_res = await client.send(upstream_req, stream=True)

    return StreamingResponse(
        stream_processor(upstream_res),
        media_type="text/event-stream"
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
