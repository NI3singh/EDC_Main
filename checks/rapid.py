"""
Rapid Round-Trip Check.

Detects withdrawal-after-deposit cycling by measuring the time gap between
the current transaction and the most recent prior DEPOSIT for the same user.

Currently only active for WITHDRAWAL transactions.

Rules:
  gap < 1 h   → rapid layering attack   → BLOCK   (score 90)
  gap < 4 h   → suspicious round-trip   → FLAG    (score 70)
  gap < 12 h  → same-day withdrawal     → MONITOR (score 45)
  gap >= 12 h → acceptable              → ALLOW   (score 0)

Uses a single indexed find_one (sorted by createdAt desc) — cheapest
possible query, no aggregation needed.
"""

from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId


# ── Thresholds ────────────────────────────────────────────────────────────────

DEPOSIT_TYPES    = {"deposit", "cashin", "cash_in", "topup", "top_up", "fund",
                    "credit", "deposit_fiat", "card_deposit", "bank_deposit"}
WITHDRAWAL_TYPES = {"withdrawal", "withdraw", "cashout", "cash_out", "payout",
                    "debit", "withdrawal_fiat", "bank_withdrawal"}


def _normalize(raw_type: str) -> str | None:
    t = (raw_type or "").strip().lower()
    if t in DEPOSIT_TYPES    or "deposit"  in t or "cashin" in t:
        return "DEPOSIT"
    if t in WITHDRAWAL_TYPES or "withdraw" in t or "cashout" in t:
        return "WITHDRAWAL"
    return None


def _deposit_regex() -> dict:
    """MongoDB $regex matching any deposit-type string."""
    return {
        "$regex":   "deposit|cashin|cash_in|topup|top_up|fund|credit",
        "$options": "i",
    }

def _make_result(
    triggered: bool,
    score: int,
    reason: str,
    details: dict | None = None,
) -> dict:
    if score >= 90:   level, action = "critical", "block"
    elif score >= 70: level, action = "high",     "flag"
    elif score >= 40: level, action = "medium",   "monitor"
    else:             level, action = "low",       "allow"
    return {
        "check":     "rapid",
        "triggered": triggered,
        "score":     score,
        "level":     level,
        "action":    action,
        "reason":    reason,
        "details":   details or {},
    }


async def check_rapid(txn: dict, db: AsyncIOMotorDatabase) -> dict:
    """
    Parameters
    ----------
    txn : full transaction document fetched from MongoDB
    db  : live Motor database handle

    Returns a CheckResult-compatible dict.
    """
    txn_type = _normalize(txn.get("type", ""))
    if txn_type != "WITHDRAWAL":
        return _make_result(False, 0, "Rapid check only applies to withdrawal transactions.")

    user_id = txn["userId"]
    txn_ts  = txn.get("createdAt") or txn.get("timestamp")

    if not txn_ts:
        return _make_result(False, 0, "Cannot determine transaction timestamp.")
    
    txn_id = txn.get("_id")
    if isinstance(txn_id, str):
        txn_id = ObjectId(txn_id)


    # ── Fetch the single most-recent OPPOSITE transaction before this one ─────
    # Exclude the current transaction itself; sort by time descending → limit 1.
    # This is a fully indexed query if (userId, type, createdAt) are indexed.
    # opposite_filter = _opposite_type_filter(txn_type)

    prev = await db["transactions"].find_one(
        {
            "userId":    user_id,
            "type":      _deposit_regex(),
            "createdAt": {"$lt": txn_ts},    # strictly before current
            "_id":       {"$ne": txn_id},
        },
        sort=[("createdAt", -1)],            # most recent first
        projection={"createdAt": 1, "finalAmount": 1, "type": 1},
    )

    if not prev:
        return _make_result(
            False, 0,
            "No prior deposit found for this user — rapid check not applicable.",
        )

    prev_ts      = prev.get("createdAt")

    if not prev_ts:
        return _make_result(False, 0, "Prior deposit has no timestamp — cannot compute gap.")
    
    gap_seconds  = (txn_ts - prev_ts).total_seconds()

    if gap_seconds is None:
        return _make_result(False, 0, "Could not compute time gap to prior transaction.")

    gap_minutes = int(gap_seconds // 60)
    gap_hours   = round(gap_seconds / 3600, 2)

    details = {
        "prev_txn_type":   prev.get("type"),
        "prev_amount":     float(prev.get("finalAmount") or 0),
        "gap_minutes":     gap_minutes,
        "gap_hours":       gap_hours,
    }

    # ════════════════════════════════════════════════════════════════════════
    # WITHDRAWAL after DEPOSIT  (the most dangerous pattern)
    # ════════════════════════════════════════════════════════════════════════
    if txn_type == "WITHDRAWAL":

        if gap_seconds < 3_600:       # < 1 hour
            return _make_result(True, 90,
                f"Rapid withdrawal: only {gap_minutes} minutes after last deposit — "
                "layering attack suspected.",
                details)

        if gap_seconds < 14_400:      # < 4 hours
            return _make_result(True, 70,
                f"Suspicious withdrawal: {gap_hours}h after last deposit — "
                "possible round-trip.",
                details)

        if gap_seconds < 43_200:      # < 12 hours
            return _make_result(True, 45,
                f"Same-day withdrawal: {gap_hours}h after last deposit — monitoring.",
                details)

        return _make_result(False, 0,
            f"Withdrawal timing is acceptable ({gap_hours}h after last deposit).",
            details)
