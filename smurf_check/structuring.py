"""
Structuring Check — 24-hour pattern analysis.

Detects:
  - Daily deposit/withdrawal hard limits
  - Fan-in smurfing (malny smal deposits → large total)
  - Reverse smurfing (many small withdrawals)
  - Hourly withdrawal velocity
  - Approaching-limit warnings

Single aggregation pipeline with $facet gives both 1h and 24h
counts + totals in one MongoDB round trip.
"""

from datetime import datetime, timedelta, timezone
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

# ── Thresholds ────────────────────────────────────────────────────────────────

DEPOSIT_DAILY_LIMIT    = 10_000.0   # $10,000 hard block
DEPOSIT_WARNING_PCT    = 0.90       # warn at 90 %
DEPOSIT_SMURF_COUNT    = 10         # min deposits to flag smurfing
DEPOSIT_SMURF_TOTAL    = 5_000.0    # min total alongside count

WITHDRAWAL_DAILY_LIMIT = 50_000.0   # $50,000 hard block
WITHDRAWAL_1H_LIMIT    = 5          # max withdrawals per hour
WITHDRAWAL_24H_LIMIT   = 12         # max withdrawals per day
WITHDRAWAL_24H_WARN    = 9          # warn at ≥9 withdrawals today

# Raw type strings the platform uses — extend freely
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
        "check":     "smurfing",
        "triggered": triggered,
        "score":     score,
        "level":     level,
        "action":    action,
        "reason":    reason,
        "details":   details or {},
    }


async def check_structuring(txn: dict, db: AsyncIOMotorDatabase) -> dict:
    """
    Parameters
    ----------
    txn : full transaction document fetched from MongoDB
    db  : live Motor database handle

    Returns a CheckResult-compatible dict.
    """
    txn_type = _normalize(txn.get("type", ""))
    if txn_type is None:
        return _make_result(False, 0, "Transaction type not subject to smurfing check.")

    amount    = float(txn.get("finalAmount") or 0)
    user_id   = txn["userId"]          # already ObjectId from Motor
    txn_ts    = txn.get("createdAt") or txn.get("timestamp")
    txn_id = txn.get("_id")
    if isinstance(txn_id, str):
        txn_id = ObjectId(txn_id)

    if not txn_ts:
        return _make_result(False, 0, "Cannot determine transaction timestamp.")

    window_24h = txn_ts - timedelta(hours=24)
    window_1h  = txn_ts - timedelta(hours=1)

    # ── Single aggregation — 1h and 24h in parallel via $facet ───────────────
    # Excludes the current transaction from the history so we can add it
    # accurately (avoids double-counting if the document is already committed).
    pipeline = [
        {
            "$match": {
                "userId": user_id,
                "type":   txn.get("type"),          # same raw type string
                "createdAt": {"$gte": window_24h, "$lt": txn_ts},  # last 24h, excluding current txn
                "_id":    {"$ne": txn_id},           # exclude current txn
            }
        },
        {
            "$facet": {
                # ── 24-hour totals ─────────────────────────────────────────
                "window_24h": [
                    {
                        "$group": {
                            "_id":   None,
                            "total": {"$sum": "$finalAmount"},
                            "count": {"$sum": 1},
                        }
                    }
                ],
                # ── 1-hour count (withdrawals only) ───────────────────────
                "window_1h": [
                    {"$match": {"createdAt": {"$gte": window_1h, "$lt": txn_ts}}},  # last 1h, excluding current txn
                    {
                        "$group": {
                            "_id":   None,
                            "count": {"$sum": 1},
                        }
                    }
                ],
            }
        },
    ]

    cursor   = db["transactions"].aggregate(pipeline, allowDiskUse=False)
    # print(cursor)
    facet    = await cursor.to_list(length=1)
    facet    = facet[0] if facet else {}
    # print(f"Facet result: {facet}")

    h24_data = facet.get("window_24h") or [{}]
    h1_data  = facet.get("window_1h")  or [{}]

    # Add current transaction to the running totals
    prior_total_24h = float((h24_data[0] or {}).get("total", 0) or 0)
    prior_count_24h = int((h24_data[0]   or {}).get("count", 0) or 0)
    prior_count_1h  = int((h1_data[0]    or {}).get("count", 0) or 0)

    total_24h = prior_total_24h + amount
    count_24h = prior_count_24h + 1
    count_1h  = prior_count_1h  + 1

    details = {
        "total_in_last_24h":  round(total_24h, 2),
        "count_in_last_24h":  count_24h,
        "count_in_last_1h":   count_1h,
        "this_amount": round(amount, 2),
    }

    # ════════════════════════════════════════════════════════════════════════
    # DEPOSIT RULES
    # ════════════════════════════════════════════════════════════════════════
    if txn_type == "DEPOSIT":

        # Rule 1 — Hard daily deposit limit
        if total_24h > DEPOSIT_DAILY_LIMIT:
            return _make_result(True, 100,
                f"Daily deposit limit exceeded: ${total_24h:,.2f} > $10,000",
                details)

        # Rule 2 — Fan-in smurfing: many deposits accumulating a large total
        if count_24h > DEPOSIT_SMURF_COUNT and total_24h > DEPOSIT_SMURF_TOTAL:
            return _make_result(True, 95,
                f"Smurfing detected: {count_24h} deposits totalling ${total_24h:,.2f}",
                details)

        # Rule 3 — Approaching daily limit (≥ 90 %)
        if total_24h >= DEPOSIT_DAILY_LIMIT * DEPOSIT_WARNING_PCT:
            return _make_result(True, 75,
                f"Approaching daily limit: ${total_24h:,.2f} of $10,000 deposited today",
                details)

        return _make_result(False, 0, "Deposit within normal parameters.", details)

    # ════════════════════════════════════════════════════════════════════════
    # WITHDRAWAL RULES
    # ════════════════════════════════════════════════════════════════════════
    if txn_type == "WITHDRAWAL":

        # Rule 1 — Hard daily withdrawal limit
        if total_24h > WITHDRAWAL_DAILY_LIMIT:
            return _make_result(True, 100,
                f"Daily withdrawal limit exceeded: ${total_24h:,.2f} > $50,000",
                details)

        # Rule 2 — Hourly velocity
        if count_1h > WITHDRAWAL_1H_LIMIT:
            return _make_result(True, 95,
                f"Withdrawal velocity exceeded: {count_1h} withdrawals in the last hour (limit: {WITHDRAWAL_1H_LIMIT})",
                details)

        # Rule 3 — Daily velocity (reverse smurfing)
        if count_24h > WITHDRAWAL_24H_LIMIT:
            return _make_result(True, 90,
                f"Reverse smurfing detected: {count_24h} withdrawals in 24 hours",
                details)

        # Rule 4 — Approaching daily velocity limit
        if count_24h >= WITHDRAWAL_24H_WARN:
            return _make_result(True, 65,
                f"High withdrawal frequency: {count_24h} withdrawals today",
                details)

        return _make_result(False, 0, "Withdrawal within normal parameters.", details)

    return _make_result(False, 0, "No smurfing rules apply.", details)


# import asyncio
# from urllib.parse import quote_plus
# from motor.motor_asyncio import AsyncIOMotorClient

# async def main():
#     # Authenticate against admin, but use playagedb for data
#     password = quote_plus("COwfPOXFOaqTTPPR4o")
#     uri = f"mongodb://playage:{password}@5.189.183.104:24018/playagedb?authSource=admin&directConnection=true"
#     client = AsyncIOMotorClient(uri)
#     db = client.get_database("playagedb")

#     # Search by transactionId field (not _id)
#     transactionId = "e77e2783-f201-4d00-9424-af9313e561cb"
#     txn = await db["transactions"].find_one({"transactionId": transactionId})

#     if txn is None:
#         # Debug: show what collections exist
#         print("Transaction not found.")
#         collections = await db.list_collection_names()
#         print(f"Collections: {collections}")

#         # Try to find by any field that might match
#         print("\nSearching by any field for this transactionId...")
#         async for t in db["transactions"].find({"transactionId": transactionId}).limit(3):
#             print(f"Found: {t}")
#         return

#     result = await check_structuring(txn, db)
#     print(result)

# asyncio.run(main())
