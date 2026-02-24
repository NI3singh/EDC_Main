"""
velocity.py — Tiered deposit velocity / sudden-change check.

Compares the current deposit against two baselines depending on how much
history exists for this user:

  Tier 1  (0-2 prior deposits)  → platform-wide percentile benchmark
  Tier 2  (3-9 prior deposits)  → hybrid: personal avg + platform benchmark
  Tier 3  (10+ prior deposits)  → full statistical analysis (z-score)

Only applies to DEPOSIT transactions.
Deposit-only history is used for the personal baseline — mixing withdrawal
amounts would distort the deposit average meaninglessly.

Every result stores the exact benchmark values that were active at evaluation
time in `details`, including a timestamp. Flags never change retroactively.
"""

from datetime import datetime, timezone, timedelta
from motor.motor_asyncio import AsyncIOMotorDatabase

from edc_main.benchmark import get_benchmark

# ── Type normalisation ────────────────────────────────────────────────────────

DEPOSIT_TYPES = {
    "deposit", "cashin", "cash_in", "topup", "top_up",
    "fund", "credit", "deposit_fiat", "card_deposit", "bank_deposit",
}

DEPOSIT_TYPE_REGEX = {
    "$regex":   "deposit|cashin|cash_in|topup|top_up|fund|credit",
    "$options": "i",
}


def _normalize(raw_type: str) -> str | None:
    t = (raw_type or "").strip().lower()
    if t in DEPOSIT_TYPES or "deposit" in t or "cashin" in t:
        return "DEPOSIT"
    return None


# ── Tier thresholds ───────────────────────────────────────────────────────────

# Tier 2 — personal history multiplier to flag
TIER2_PERSONAL_MULTIPLIER = 4.0    # amount > 4× personal avg → suspicious

# Tier 3 — statistical z-score thresholds
TIER3_EXTREME_MULTIPLIER  = 5.0    # amount > 5× avg → extreme spike
TIER3_SIGMA_BLOCK         = 3.0    # z-score ≥ 3.0 → flag (score 80)
TIER3_SIGMA_MONITOR       = 2.0    # z-score ≥ 2.0 → monitor (score 55)

# Frequency — how many deposits today vs historical daily rate
FREQUENCY_SPIKE_MULTIPLIER = 3.0   # today's count > 3× daily avg → flag

# History windows
HISTORY_DAYS_PERSONAL = 90
HISTORY_DAYS_FREQUENCY = 30
TIER2_MIN = 3
TIER3_MIN = 10


# ── Result builder ────────────────────────────────────────────────────────────

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

    # Always stamp when this evaluation happened
    base_details = {"evaluated_at": datetime.now(timezone.utc).isoformat()}
    if details:
        base_details.update(details)

    return {
        "check":     "velocity",
        "triggered": triggered,
        "score":     score,
        "level":     level,
        "action":    action,
        "reason":    reason,
        "details":   base_details,
    }


# ── Main check ────────────────────────────────────────────────────────────────

async def check_velocity(txn: dict, db: AsyncIOMotorDatabase) -> dict:
    """
    Parameters
    ----------
    txn : full transaction document fetched from MongoDB
    db  : live Motor database handle

    Returns a CheckResult-compatible dict.
    """
    # Only runs for deposits
    if _normalize(txn.get("type", "")) != "DEPOSIT":
        return _make_result(False, 0, "Velocity check only applies to deposit transactions.")

    amount  = float(txn.get("finalAmount") or 0)
    user_id = txn["userId"]
    txn_ts  = txn.get("createdAt") or txn.get("timestamp")
    txn_id  = txn["_id"]

    if not txn_ts:
        return _make_result(False, 0, "Cannot determine transaction timestamp.")

    if amount <= 0:
        return _make_result(False, 0, "Zero or negative amount — skipping velocity check.")

    # ── Load benchmark (in-memory, no DB call) ────────────────────────────────
    bm = get_benchmark()

    bm_snapshot = {
        "platform_p75":         bm.get("p75"),
        "platform_p90":         bm.get("p90"),
        "platform_p95":         bm.get("p95"),
        "platform_p99":         bm.get("p99"),
        "platform_total_deposits": bm.get("total_deposits"),
        "benchmark_computed_at": bm.get("computed_at").isoformat()
                                  if bm.get("computed_at") else None,
    }

    # ── Count prior deposits for this user (single lightweight query) ─────────
    history_start = txn_ts - timedelta(days=HISTORY_DAYS_PERSONAL)

    prior_count = await db["transactions"].count_documents({
        "userId":    user_id,
        "type":      DEPOSIT_TYPE_REGEX,
        "createdAt": {"$gte": history_start},
        "_id":       {"$ne": txn_id},
    })

    base = {
        "tier":               _get_tier(prior_count),
        "prior_deposit_count": prior_count,
        "this_amount":        round(amount, 2),
        **bm_snapshot,
    }

    # ── Route to correct tier ─────────────────────────────────────────────────
    if prior_count < TIER2_MIN:
        return await _tier1(amount, txn, db, base, bm)

    if prior_count < TIER3_MIN:
        return await _tier2(amount, user_id, txn_id, txn_ts, db, base, bm, prior_count)

    return await _tier3(amount, user_id, txn_id, txn_ts, db, base, bm)


# ── Tier helpers ──────────────────────────────────────────────────────────────

def _get_tier(count: int) -> int:
    if count < TIER2_MIN:  return 1
    if count < TIER3_MIN:  return 2
    return 3


# ── Tier 1 — New users: compare against platform benchmark only ───────────────

async def _tier1(
    amount: float,
    txn: dict,
    db: AsyncIOMotorDatabase,
    base: dict,
    bm: dict,
) -> dict:
    """
    0-2 prior deposits. No personal baseline exists.
    Compare only against platform-wide percentiles.
    """
    if not bm.get("ready"):
        return _make_result(False, 0,
            "Platform benchmark not yet initialised — velocity check skipped.",
            base)

    p99 = bm.get("p99") or 0
    p95 = bm.get("p95") or 0
    p90 = bm.get("p90") or 0

    # First-ever deposit above p99 — strongest new-user signal
    if base["prior_deposit_count"] == 0 and p99 > 0 and amount > p99:
        return _make_result(True, 85,
            f"First-ever deposit of ${amount:,.2f} exceeds platform 99th percentile "
            f"(${p99:,.2f}). No prior history to contextualise.",
            {**base, "rule": "first_deposit_above_p99"})

    # Any Tier 1 user above p99
    if p99 > 0 and amount > p99:
        return _make_result(True, 75,
            f"Deposit of ${amount:,.2f} exceeds platform 99th percentile (${p99:,.2f}) "
            f"with only {base['prior_deposit_count']} prior deposit(s) on record.",
            {**base, "rule": "tier1_above_p99"})

    # Above p95 — flag, not block
    if p95 > 0 and amount > p95:
        return _make_result(True, 65,
            f"Deposit of ${amount:,.2f} exceeds platform 95th percentile (${p95:,.2f}) "
            f"with only {base['prior_deposit_count']} prior deposit(s) on record.",
            {**base, "rule": "tier1_above_p95"})

    # Above p90 — monitor only
    if p90 > 0 and amount > p90:
        return _make_result(True, 42,
            f"Deposit of ${amount:,.2f} is above platform 90th percentile (${p90:,.2f}). "
            "Monitoring — limited personal history.",
            {**base, "rule": "tier1_above_p90"})

    return _make_result(False, 0,
        f"Deposit of ${amount:,.2f} is within normal platform range. "
        f"User has {base['prior_deposit_count']} prior deposit(s).",
        base)


# ── Tier 2 — Low history: hybrid personal + platform ─────────────────────────

async def _tier2(
    amount: float,
    user_id,
    txn_id,
    txn_ts: datetime,
    db: AsyncIOMotorDatabase,
    base: dict,
    bm: dict,
    prior_count: int,
) -> dict:
    """
    3-9 prior deposits. Thin personal baseline — require BOTH personal and
    platform signals to fire before flagging. One signal alone = monitor.
    """
    history_start  = txn_ts - timedelta(days=HISTORY_DAYS_PERSONAL)
    today_start    = txn_ts.replace(hour=0, minute=0, second=0, microsecond=0)
    freq_start     = txn_ts - timedelta(days=HISTORY_DAYS_FREQUENCY)

    # Single $facet: personal avg + today count + 30-day daily frequency
    pipeline = [
        {
            "$match": {
                "userId":    user_id,
                "type":      DEPOSIT_TYPE_REGEX,
                "createdAt": {"$gte": history_start},
                "_id":       {"$ne": txn_id},
            }
        },
        {
            "$facet": {
                "personal": [
                    {
                        "$group": {
                            "_id": None,
                            "avg": {"$avg": "$finalAmount"},
                            "max": {"$max": "$finalAmount"},
                        }
                    }
                ],
                "today": [
                    {"$match": {"createdAt": {"$gte": today_start}}},
                    {"$group": {"_id": None, "count": {"$sum": 1}}}
                ],
                "freq_30d": [
                    {"$match": {"createdAt": {"$gte": freq_start}}},
                    {
                        "$group": {
                            "_id": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date":   "$createdAt",
                                }
                            },
                            "daily_count": {"$sum": 1},
                        }
                    },
                    {
                        "$group": {
                            "_id":           None,
                            "avg_daily":     {"$avg": "$daily_count"},
                            "active_days":   {"$sum": 1},
                        }
                    }
                ],
            }
        },
    ]

    cursor = db["transactions"].aggregate(pipeline, allowDiskUse=False)
    rows   = await cursor.to_list(length=1)
    facet  = rows[0] if rows else {}

    personal_row  = (facet.get("personal")  or [{}])[0] or {}
    today_row     = (facet.get("today")     or [{}])[0] or {}
    freq_row      = (facet.get("freq_30d")  or [{}])[0] or {}

    personal_avg  = float(personal_row.get("avg") or 0)
    personal_max  = float(personal_row.get("max") or 0)
    today_count   = int(today_row.get("count", 0) or 0) + 1    # include current
    avg_daily     = float(freq_row.get("avg_daily", 0) or 0)
    active_days   = int(freq_row.get("active_days", 0) or 0)

    p90 = bm.get("p90") or 0
    p99 = bm.get("p99") or 0

    details = {
        **base,
        "personal_avg":    round(personal_avg, 2),
        "personal_max":    round(personal_max, 2),
        "today_count":     today_count,
        "avg_daily_freq":  round(avg_daily, 2),
        "active_days_30d": active_days,
    }

    # ── Amount checks ─────────────────────────────────────────────────────────

    personal_spike = personal_avg > 0 and amount > (personal_avg * TIER2_PERSONAL_MULTIPLIER)
    platform_high  = p90 > 0 and amount > p90

    # Both fire → flag
    if personal_spike and platform_high:
        return _make_result(True, 78,
            f"Deposit ${amount:,.2f} is {amount/personal_avg:.1f}× this user's average "
            f"(${personal_avg:,.2f}) and exceeds platform 90th percentile (${p90:,.2f}).",
            {**details, "rule": "tier2_both_signals"})

    # Above p99 alone → flag regardless of personal
    if p99 > 0 and amount > p99:
        return _make_result(True, 72,
            f"Deposit ${amount:,.2f} exceeds platform 99th percentile (${p99:,.2f}) "
            f"(user has {prior_count} prior deposits).",
            {**details, "rule": "tier2_above_p99"})

    # Only personal fires → monitor
    if personal_spike:
        return _make_result(True, 52,
            f"Deposit ${amount:,.2f} is {amount/personal_avg:.1f}× this user's "
            f"90-day average (${personal_avg:,.2f}). Monitoring.",
            {**details, "rule": "tier2_personal_only"})

    # Only platform fires → monitor
    if platform_high:
        return _make_result(True, 44,
            f"Deposit ${amount:,.2f} exceeds platform 90th percentile (${p90:,.2f}). "
            "Limited personal history — monitoring.",
            {**details, "rule": "tier2_platform_only"})

    # ── Frequency check ───────────────────────────────────────────────────────
    freq_result = _check_frequency(today_count, avg_daily, active_days, details)
    if freq_result:
        return freq_result

    return _make_result(False, 0,
        f"Deposit ${amount:,.2f} is within normal range for this user and the platform.",
        details)


# ── Tier 3 — Established users: full statistical analysis ────────────────────

async def _tier3(
    amount: float,
    user_id,
    txn_id,
    txn_ts: datetime,
    db: AsyncIOMotorDatabase,
    base: dict,
    bm: dict,
) -> dict:
    """
    10+ prior deposits. Full z-score + daily frequency analysis.
    """
    history_start = txn_ts - timedelta(days=HISTORY_DAYS_PERSONAL)
    today_start   = txn_ts.replace(hour=0, minute=0, second=0, microsecond=0)
    freq_start    = txn_ts - timedelta(days=HISTORY_DAYS_FREQUENCY)

    pipeline = [
        {
            "$match": {
                "userId":    user_id,
                "type":      DEPOSIT_TYPE_REGEX,
                "createdAt": {"$gte": history_start},
                "_id":       {"$ne": txn_id},
            }
        },
        {
            "$facet": {
                # Per-transaction statistics
                "per_txn": [
                    {
                        "$group": {
                            "_id":    None,
                            "avg":    {"$avg":       "$finalAmount"},
                            "stddev": {"$stdDevPop": "$finalAmount"},
                            "count":  {"$sum":       1},
                            "max":    {"$max":       "$finalAmount"},
                        }
                    }
                ],
                # Today's deposits so far
                "today": [
                    {"$match": {"createdAt": {"$gte": today_start}}},
                    {"$group": {"_id": None, "count": {"$sum": 1}}}
                ],
                # 30-day daily frequency baseline
                "freq_30d": [
                    {"$match": {"createdAt": {"$gte": freq_start}}},
                    {
                        "$group": {
                            "_id": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date":   "$createdAt",
                                }
                            },
                            "daily_count": {"$sum": 1},
                        }
                    },
                    {
                        "$group": {
                            "_id":         None,
                            "avg_daily":   {"$avg": "$daily_count"},
                            "active_days": {"$sum": 1},
                        }
                    }
                ],
            }
        },
    ]

    cursor = db["transactions"].aggregate(pipeline, allowDiskUse=False)
    rows   = await cursor.to_list(length=1)
    facet  = rows[0] if rows else {}

    per_txn_row = (facet.get("per_txn")  or [{}])[0] or {}
    today_row   = (facet.get("today")    or [{}])[0] or {}
    freq_row    = (facet.get("freq_30d") or [{}])[0] or {}

    avg         = float(per_txn_row.get("avg",    0) or 0)
    stddev      = float(per_txn_row.get("stddev", 0) or 0)
    count       = int(per_txn_row.get("count",    0) or 0)
    today_count = int(today_row.get("count",       0) or 0) + 1
    avg_daily   = float(freq_row.get("avg_daily",  0) or 0)
    active_days = int(freq_row.get("active_days",  0) or 0)

    details = {
        **base,
        "personal_avg":          round(avg,         2),
        "personal_stddev":       round(stddev,       2),
        "prior_count_in_window": count,
        "today_count":           today_count,
        "avg_daily_freq":        round(avg_daily,    2),
        "active_days_30d":       active_days,
    }

    # ── Rule 1: Extreme multiplier spike ─────────────────────────────────────
    if avg > 0 and amount > avg * TIER3_EXTREME_MULTIPLIER:
        return _make_result(True, 85,
            f"Extreme deposit spike: ${amount:,.2f} is "
            f"{amount/avg:.1f}× this user's 90-day average (${avg:,.2f}).",
            {**details, "rule": "tier3_extreme_multiplier",
             "ratio_to_avg": round(amount / avg, 2)})

    # ── Rule 2: 3-sigma statistical outlier ───────────────────────────────────
    if stddev > 0 and amount > avg + (TIER3_SIGMA_BLOCK * stddev):
        z = (amount - avg) / stddev
        return _make_result(True, 80,
            f"Statistical outlier: ${amount:,.2f} is {z:.1f}σ above this user's "
            f"mean (avg=${avg:,.2f}, σ=${stddev:,.2f}).",
            {**details, "rule": "tier3_3sigma", "z_score": round(z, 2)})

    # ── Rule 3: 2-sigma notable deviation ────────────────────────────────────
    if stddev > 0 and amount > avg + (TIER3_SIGMA_MONITOR * stddev):
        z = (amount - avg) / stddev
        return _make_result(True, 55,
            f"Notable deviation: ${amount:,.2f} is {z:.1f}σ above this user's "
            f"mean (avg=${avg:,.2f}, σ=${stddev:,.2f}). Monitoring.",
            {**details, "rule": "tier3_2sigma", "z_score": round(z, 2)})

    # ── Rule 4: Frequency spike ───────────────────────────────────────────────
    freq_result = _check_frequency(today_count, avg_daily, active_days, details)
    if freq_result:
        return freq_result

    return _make_result(False, 0,
        f"Deposit ${amount:,.2f} is within this user's normal range "
        f"(avg=${avg:,.2f} ± ${stddev:,.2f}).",
        details)


# ── Shared frequency check ────────────────────────────────────────────────────

def _check_frequency(
    today_count: int,
    avg_daily: float,
    active_days: int,
    details: dict,
) -> dict | None:
    """
    Flags if today's deposit count is significantly above this user's
    normal daily deposit frequency.
    Returns a result dict if triggered, None if clean.
    Requires at least 5 active days of history to be meaningful.
    """
    if active_days < 5 or avg_daily <= 0:
        return None

    if today_count > avg_daily * FREQUENCY_SPIKE_MULTIPLIER:
        return _make_result(True, 62,
            f"Unusual deposit frequency: {today_count} deposits today vs "
            f"this user's daily average of {avg_daily:.1f}. "
            f"({today_count/avg_daily:.1f}× normal rate).",
            {**details, "rule": "frequency_spike",
             "ratio_to_avg_frequency": round(today_count / avg_daily, 2)})

    return None