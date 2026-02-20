from typing import Any
from pydantic import BaseModel, Field


class EDCRequest(BaseModel):
    transaction_id: str = Field(..., min_length=1, description="i-betting platform transactionId")

    class Config:
        json_schema_extra = {"example": {"transaction_id": "txn_67890abc"}}


class CheckResult(BaseModel):
    check: str          # structuring | rapid | velocity
    triggered: bool
    score: int = Field(ge=0, le=100)
    level: str          # low | medium | high | critical
    action: str         # allow | monitor | flag | block
    reason: str
    details: dict[str, Any] = {}


class EDCResponse(BaseModel):
    transaction_id: str
    user_id: str
    txn_type: str       # DEPOSIT | WITHDRAWAL | OTHER
    amount: float

    # Aggregated verdict
    final_score: int = Field(ge=0, le=100)
    final_level: str
    final_action: str

    # Per-check detail
    checks: list[CheckResult]

    class Config:
        json_schema_extra = {
            "example": {
                "transaction_id": "txn_abc",
                "user_id": "user_123",
                "txn_type": "DEPOSIT",
                "amount": 5000.0,
                "final_score": 95,
                "final_level": "critical",
                "final_action": "block",
                "checks": []
            }
        }
