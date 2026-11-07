#!/usr/bin/env python3
import json
import httpx
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

app = FastAPI(title="MCP Security Gateway")

DOWNSTREAM_MCP_URL = "http://127.0.0.1:8001/mcp"

# Mock token claims: Token -> Role
MOCK_ROLE_TOKENS = {
    "admin-secret-token": "admin",
    "viewer-secret-token": "viewer"
}

def extract_role_from_header(auth_header: str | None) -> str | None:
    if not auth_header or not auth_header.startswith("Bearer "):
        return None
    token = auth_header.split(" ", 1)[1].strip()
    return MOCK_ROLE_TOKENS.get(token)

def jsonrpc_error_response(request_id: str | int | None, code: int, message: str):
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": code,
                "message": message
            }
        }
    )

@app.post("/mcp")
async def mcp_proxy(request: Request):
    auth_header = request.headers.get("Authorization")
    user_role = extract_role_from_header(auth_header)

    try:
        payload = await request.json()
    except Exception:
        return jsonrpc_error_response(None, -32700, "Parse Error: Invalid JSON")

    req_id = payload.get("id")
    method = payload.get("method")
    params = payload.get("params", {})

    # Intercept and validate tool executions
    if method == "tools/call":
        tool_name = params.get("name", "")
        if tool_name.startswith("admin_"):
            if user_role != "admin":
                return jsonrpc_error_response(
                    req_id,
                    -32001,
                    f"Unauthorized Tool Call: '{tool_name}' requires 'admin' role"
                )

    # Forward authorized calls & transparent methods (e.g. tools/list) downstream
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            downstream_res = await client.post(
                DOWNSTREAM_MCP_URL,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            return JSONResponse(
                content=downstream_res.json(),
                status_code=downstream_res.status_code
            )
        except httpx.RequestError as exc:
            return jsonrpc_error_response(
                req_id,
                -32603,
                f"Internal Gateway Error: Downstream server unreachable: {str(exc)}"
            )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
