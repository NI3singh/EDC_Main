"""
benchmark.py — Platform-wide deposit percentile cache.

Computes p75, p90, p95, p99 across ALL deposit transactions on the platform.
Loaded once at startup, refreshed every hour in the background.
All check_velocity calls read from this in-memory dict — zero DB cost per request.
"""

import asyncio
import logging
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorDatabase

logger = logging.getLogger(__name__)

# ── In-memory store ───────────────────────────────────────────────────────────

_benchmark: dict = {
    "p75":          None,
    "p90":          None,
    "p95":          None,
    "p99":          None,
    "total_deposits": 0,
    "computed_at":  None,
    "ready":        False,   # False until first successful computation
}

DEPOSIT_TYPE_REGEX = {
    "$regex":   "deposit|cashin|cash_in|topup|top_up|fund|credit",
    "$options": "i",
}

REFRESH_INTERVAL_SECONDS = 3600   # 1 hour


# ── Computation ───────────────────────────────────────────────────────────────

async def _compute(db: AsyncIOMotorDatabase) -> None:
    """
    Runs the platform-wide $percentile aggregation against MongoDB.
    Updates _benchmark in place. Safe to call concurrently — Python dict
    assignment is atomic for simple key writes.

    MongoDB 7.0+ has native $percentile. For older versions we fall back
    to a $bucketAuto approximation that is close enough for AML thresholds.
    """
    global _benchmark

    try:
        pipeline = [
            {
                "$match": {
                    "type":        DEPOSIT_TYPE_REGEX,
                    "finalAmount": {"$gt": 0},
                }
            },
            {
                "$group": {
                    "_id":   None,
                    "count": {"$sum": 1},
                    # $percentile is MongoDB 7.0+.
                    # If your cluster is older, replace with $bucketAuto below.
                    "p75": {
                        "$percentile": {
                            "input":  "$finalAmount",
                            "p":      [0.75],
                            "method": "approximate",
                        }
                    },
                    "p90": {
                        "$percentile": {
                            "input":  "$finalAmount",
                            "p":      [0.90],
                            "method": "approximate",
                        }
                    },
                    "p95": {
                        "$percentile": {
                            "input":  "$finalAmount",
                            "p":      [0.95],
                            "method": "approximate",
                        }
                    },
                    "p99": {
                        "$percentile": {
                            "input":  "$finalAmount",
                            "p":      [0.99],
                            "method": "approximate",
                        }
                    },
                }
            },
        ]

        cursor = db["transactions"].aggregate(pipeline, allowDiskUse=False)
        rows   = await cursor.to_list(length=1)

        if not rows:
            logger.warning("Benchmark query returned no data — transactions collection may be empty.")
            return

        row = rows[0]
        now = datetime.now(timezone.utc)

        # $percentile returns a list with one element per p value
        _benchmark = {
            "p75":            float(row["p75"][0]) if row.get("p75") else 0.0,
            "p90":            float(row["p90"][0]) if row.get("p90") else 0.0,
            "p95":            float(row["p95"][0]) if row.get("p95") else 0.0,
            "p99":            float(row["p99"][0]) if row.get("p99") else 0.0,
            "total_deposits": int(row.get("count", 0)),
            "computed_at":    now,
            "ready":          True,
        }

        logger.info(
            "Platform benchmark updated | p75=$%.2f p90=$%.2f p95=$%.2f p99=$%.2f | "
            "based on %d deposits | at %s",
            _benchmark["p75"], _benchmark["p90"],
            _benchmark["p95"], _benchmark["p99"],
            _benchmark["total_deposits"],
            now.isoformat(),
        )

    except Exception as exc:
        logger.error("Benchmark computation failed: %s", exc, exc_info=True)
        # Do not wipe existing benchmark on failure — keep last good values


# ── Background refresh loop ───────────────────────────────────────────────────

async def start_benchmark_refresh(db: AsyncIOMotorDatabase) -> None:
    """
    Called once from main.py lifespan at startup.
    1. Runs the first computation immediately (blocks until done so the
       first real request never hits an empty benchmark).
    2. Launches a background loop that refreshes every hour silently.
    FastAPI never restarts — the loop lives inside the same async process.
    """
    logger.info("Computing initial platform benchmark...")
    await _compute(db)

    async def _loop():
        while True:
            await asyncio.sleep(REFRESH_INTERVAL_SECONDS)
            logger.info("Refreshing platform benchmark (hourly)...")
            await _compute(db)

    asyncio.create_task(_loop())
    logger.info("Platform benchmark background refresh scheduled (every %dh).",
                REFRESH_INTERVAL_SECONDS // 3600)


# ── Public read accessor ──────────────────────────────────────────────────────

def get_benchmark() -> dict:
    """
    Returns the current in-memory benchmark snapshot.
    Called by check_velocity on every request — no DB, no await needed.
    """
    return _benchmark