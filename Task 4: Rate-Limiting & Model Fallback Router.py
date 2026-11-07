#!/usr/bin/env python3
import asyncio
import time
import sqlite3
import httpx
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

app = FastAPI(title="Resilient LLM Routing Gateway")

SQLITE_DB_PATH = "gateway_ratelimit.db"
MAX_TOKENS_PER_MINUTE = 50000
TIMEOUT_SECONDS = 3.0

PRIMARY_MODEL_URL = "http://127.0.0.1:9001/v1/chat/completions"
SECONDARY_MODEL_URL = "http://127.0.0.1:9002/v1/chat/completions"

# --- On-Disk SQLite Rate Limiter ---
def init_db():
    with sqlite3.connect(SQLITE_DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS token_ledger (
                api_key TEXT NOT NULL,
                tokens INTEGER NOT NULL,
                timestamp REAL NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_key_ts ON token_ledger(api_key, timestamp)")
        conn.commit()

init_db()

def check_and_record_rate_limit(api_key: str, estimated_tokens: int) -> bool:
    """Sliding-window token verification using SQLite."""
    current_time = time.time()
    one_minute_ago = current_time - 60.0

    with sqlite3.connect(SQLITE_DB_PATH) as conn:
        cursor = conn.cursor()
        # Evict records older than 1 minute
        cursor.execute("DELETE FROM token_ledger WHERE timestamp < ?", (one_minute_ago,))
        
        # Calculate tokens consumed in sliding window
        cursor.execute(
            "SELECT COALESCE(SUM(tokens), 0) FROM token_ledger WHERE api_key = ? AND timestamp >= ?",
            (api_key, one_minute_ago)
        )
        consumed = cursor.fetchone()[0]

        if (consumed + estimated_tokens) > MAX_TOKENS_PER_MINUTE:
            conn.commit()
            return False

        # Record consumption
        cursor.execute(
            "INSERT INTO token_ledger (api_key, tokens, timestamp) VALUES (?, ?, ?)",
            (api_key, estimated_tokens, current_time)
        )
        conn.commit()
        return True

def sanitize_error(code: str, message: str, http_status: int = status.HTTP_502_BAD_GATEWAY):
    return JSONResponse(
        status_code=http_status,
        content={
            "gateway_error": {
                "code": code,
                "message": message,
                "timestamp": int(time.time())
            }
        }
    )

async def dispatch_with_timeout(client: httpx.AsyncClient, url: str, payload: dict) -> httpx.Response:
    return await client.post(url, json=payload, timeout=TIMEOUT_SECONDS)

@app.post("/v1/chat/completions")
async def route_completion(request: Request):
    api_key = request.headers.get("x-api-key")
    if not api_key:
        return sanitize_error("MISSING_API_KEY", "Missing 'x-api-key' header.", status.HTTP_401_UNAUTHORIZED)

    try:
        body = await request.json()
    except Exception:
        return sanitize_error("INVALID_PAYLOAD", "Malformed JSON body.", status.HTTP_400_BAD_REQUEST)

    # Estimate tokens: ~1 token per 4 characters for prompts + allocated max_tokens
    prompt_chars = sum(len(m.get("content", "")) for m in body.get("messages", []))
    estimated_tokens = (prompt_chars // 4) + body.get("max_tokens", 256)

    # 1. Sliding Window Rate Limiting
    is_allowed = check_and_record_rate_limit(api_key, estimated_tokens)
    if not is_allowed:
        return sanitize_error(
            "RATE_LIMIT_EXCEEDED",
            f"Tenant exceeded throughput limit of {MAX_TOKENS_PER_MINUTE} tokens/min.",
            status.HTTP_429_TOO_MANY_REQUESTS
        )

    # 2. Resilient Execution & Fallback Mechanism
    async with httpx.AsyncClient() as client:
        primary_failed = False

        try:
            resp = await dispatch_with_timeout(client, PRIMARY_MODEL_URL, body)
            if resp.status_code == 429:
                primary_failed = True
            elif resp.is_success:
                return JSONResponse(content=resp.json(), status_code=resp.status_code)
            else:
                primary_failed = True
        except (httpx.TimeoutException, httpx.RequestError):
            primary_failed = True

        # Secondary Failover
        if primary_failed:
            try:
                backup_resp = await dispatch_with_timeout(client, SECONDARY_MODEL_URL, body)
                if backup_resp.is_success:
                    data = backup_resp.json()
                    data["_routed_to"] = "secondary_backup"
                    return JSONResponse(content=data, status_code=backup_resp.status_code)
                else:
                    return sanitize_error("UPSTREAM_PROVIDER_ERROR", "Backup provider unavailable.")
            except httpx.TimeoutException:
                return sanitize_error("GATEWAY_TIMEOUT", "All model endpoints timed out after threshold.")
            except Exception:
                return sanitize_error("SERVICE_UNAVAILABLE", "Downstream service error during failover.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
