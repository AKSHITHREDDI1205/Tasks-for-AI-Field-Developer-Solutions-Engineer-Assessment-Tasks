#!/usr/bin/env python3
import sys
import logging
from pydantic import BaseModel, Field, ValidationError
from mcp.server.fastmcp import FastMCP
from mcp.shared.exceptions import McpError
from mcp.types import ErrorData, INVALID_PARAMS, INTERNAL_ERROR

# Configure logging exclusively to STDERR to ensure STDOUT remains pure JSON-RPC
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("mcp_server")

# Initialize FastMCP Server
mcp = FastMCP("CustomerSupportServer")

# --- Validation Schemas ---
class CustomerRecordInput(BaseModel):
    customer_id: str = Field(
        ...,
        pattern=r"^CUST-\d{5}$",
        description="Formatted customer identifier (e.g. CUST-12345)"
    )

class TriggerRefundInput(BaseModel):
    customer_id: str = Field(..., pattern=r"^CUST-\d{5}$")
    amount: float = Field(..., gt=0.0, description="Refund amount, must be positive float")
    reason: str = Field(..., min_length=10, description="Refund rationale, min 10 chars")

# --- In-Memory State Mock ---
CUSTOMER_DB = {
    "CUST-10001": {"name": "Alice Smith", "tier": "Enterprise", "balance": 1500.00},
    "CUST-10002": {"name": "Bob Jones", "tier": "Pro", "balance": 450.50},
}

# --- Tool Definitions ---
@mcp.tool()
async def get_customer_record(customer_id: str) -> dict:
    """Fetch customer details by customer ID."""
    logger.info("Executing tool: get_customer_record for customer_id: %s", customer_id)
    try:
        validated = CustomerRecordInput(customer_id=customer_id)
    except ValidationError as err:
        logger.error("Schema validation failed: %s", err)
        raise McpError(
            ErrorData(
                code=INVALID_PARAMS,
                message=f"Invalid customer_id format. Expected CUST-XXXXX: {err.errors()}"
            )
        )

    record = CUSTOMER_DB.get(validated.customer_id)
    if not record:
        return {"found": False, "customer_id": validated.customer_id, "data": None}

    return {"found": True, "customer_id": validated.customer_id, "data": record}

@mcp.tool()
async def trigger_refund(customer_id: str, amount: float, reason: str) -> dict:
    """Issue a refund to a specified customer."""
    logger.info("Executing tool: trigger_refund for %s (amount: %f)", customer_id, amount)
    try:
        validated = TriggerRefundInput(
            customer_id=customer_id,
            amount=amount,
            reason=reason
        )
    except ValidationError as err:
        logger.error("Schema validation failed: %s", err)
        raise McpError(
            ErrorData(
                code=INVALID_PARAMS,
                message=f"Validation failed for trigger_refund: {err.errors()}"
            )
        )

    # Process refund business logic
    return {
        "status": "APPROVED",
        "customer_id": validated.customer_id,
        "refund_id": "RFND-98231",
        "processed_amount": validated.amount,
        "reason": validated.reason
    }

if __name__ == "__main__":
    logger.info("Starting CustomerSupport MCP server over stdio...")
    mcp.run(transport="stdio")
