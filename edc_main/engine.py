"""
engine.py — EDC orchestration layer.

Fetches the transaction once, then runs all checks in parallel.
Adding a new check is two lines: import it, add it to _CHECKS.
"""

import asyncio
from motor.motor_asyncio import AsyncIOMotorDatabase

from checks.structuring import check_structuring
from checks.rapid import check_rapid
from checks.velocity import check_velocity



# ── Register checks here — order does not matter (all run in parallel) ────────
_CHECKS = [check_structuring, check_rapid, check_velocity]


def _aggregate(results: list[dict]) -> tuple[int, str, str]:
    """
    Pattern 1 — max-severity wins.

    block  if any check says block
    flag   if any check says flag
    monitor if any check says monitor
    allow  otherwise
    """
    actions = [r["action"] for r in results]
    scores  = [r["score"]  for r in results]
    triggered_by = [r["check"]   for r in results if r.get("triggered")]

    final_score   = max(scores) if scores else 0

    if "block"   in actions: return final_score, "critical", "block",   triggered_by
    if "flag"    in actions: return final_score, "high",     "flag",    triggered_by
    if "monitor" in actions: return final_score, "medium",   "monitor", triggered_by
    return final_score, "low", "allow", triggered_by

# ── Fallback result for a check that raises an exception ──────────────────────

def _error_result(check_name: str, exc: Exception) -> dict:
    """
    If a check throws an unhandled exception we degrade gracefully to monitor
    rather than crashing the entire request and losing the other checks.
    The error is surfaced in details for debugging.
    """
    return {
        "check":     check_name,
        "triggered": True,           # treat as triggered so it is not silently ignored
        "score":     50,
        "level":     "medium",
        "action":    "monitor",
        "reason":    f"{check_name} check encountered an internal error — manual review required.",
        "details":   {"error": str(exc), "error_type": type(exc).__name__},
    }

async def run_edc(transaction_id: str, db: AsyncIOMotorDatabase) -> dict:
    """
    Main entry point called by the API route.

    1. Fetch transaction from i-betting platform MongoDB.
    2. Run all registered checks in parallel.
    3. Aggregate with Pattern 1.
    4. Return unified result dict.
    """
    # ── Step 1: Fetch transaction ─────────────────────────────────────────────
    txn = await db["transactions"].find_one({"transactionId": transaction_id})
    if not txn:
        raise ValueError(f"Transaction '{transaction_id}' not found.")

    # ── Step 2: Run all checks in parallel ────────────────────────────────────
    raw = await asyncio.gather(
        *[fn(txn, db) for fn in _CHECKS],
        return_exceptions=True,
    )

    results = []
    for fn, outcome in zip(_CHECKS, raw):
        if isinstance(outcome, Exception):
            results.append(_error_result(fn.__name__.replace("check_", ""), outcome))
        else:
            results.append(outcome)

    # ── Step 3: Aggregate ─────────────────────────────────────────────────────
    final_score, final_level, final_action, triggered_by = _aggregate(results)

    # ── Step 4: Build response ────────────────────────────────────────────────
    amount  = float(txn.get("finalAmount") or 0)
    raw_type = str(txn.get("type") or "")

    return {
        "transaction_id": transaction_id,
        "user_id":        str(txn.get("userId", "")),
        "txn_type":       raw_type,
        "amount":         round(amount, 2),
        "final_score":    final_score,
        "final_level":    final_level,
        "final_action":   final_action,
        "triggered_by":   triggered_by,
        "checks":         results,
    }
