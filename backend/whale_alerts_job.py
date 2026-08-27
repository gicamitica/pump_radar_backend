"""
whale_alerts_job.py - Background whale movement alert feed.
NEW module, isolated. Does not import/modify scanner.py, judge.py, enricher.py, snapshot.py.

Runs hourly (registered as a separate scheduler job in server.py, does not touch
the existing 'crypto_signals' hourly job). Pulls currently active ETH pump/dex
signals (read-only, via own OSINT-style HTTP call to the local API), re-uses the
whale_movements.py detection logic directly (no duplication), and persists any
MEDIUM/HIGH verdict into MongoDB so the frontend feed survives restarts.
"""
import time
import asyncio
from typing import Dict, Any, List

import requests
from fastapi import APIRouter
import logging

logger = logging.getLogger('whale_alerts')
from motor.motor_asyncio import AsyncIOMotorClient
import os

from whale_movements import _analyze_sync, _get_haiku_verdict

router = APIRouter(prefix="/api/crypto/whale-alerts", tags=["whale-alerts"])

OWN_SIGNALS_BASE = "http://127.0.0.1:8020"
MAX_TOKENS_PER_RUN = 15  # keep Etherscan/Haiku load bounded, matches existing module limits
DEDUP_WINDOW_SECONDS = 3600  # one alert per token per hour

_mongo_client: AsyncIOMotorClient = None


def _get_db():
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = AsyncIOMotorClient(os.environ["MONGO_URL"])
    return _mongo_client[os.environ["DB_NAME"]]


async def _get_active_eth_tokens(db, limit: int = MAX_TOKENS_PER_RUN) -> List[Dict[str, str]]:
    """Citeste direct din MongoDB (nu HTTP self-request care deadlock-uia pe single worker)."""
    try:
        snap = await db.signal_snapshots_v2.find_one(sort=[("timestamp", -1)])
        if not snap:
            return []
        seen = set()
        out = []
        for cat in ("pump_signals", "dex_signals", "watch_signals", "early_signals", "risk_signals", "dump_signals"):
            for it in (snap.get(cat) or []):
                if (it.get("network") or "").lower() not in ("eth", "ethereum"):
                    continue
                addr = (it.get("token_address") or "").lower()
                if not addr or addr in seen:
                    continue
                seen.add(addr)
                out.append({"symbol": it.get("symbol", ""), "address": addr})
                if len(out) >= limit:
                    return out
        return out
    except Exception as exc:
        logger.warning(f"_get_active_eth_tokens error: {exc}")
        return []


async def run_whale_alert_scan(db):
    """Called by the scheduler. Isolated from the main scan pipeline."""
    logger.info("whale_alerts_scan: starting run")
    tokens = await _get_active_eth_tokens(db)
    logger.info(f"whale_alerts_scan: {len(tokens)} active ETH tokens found")
    if not tokens:
        logger.info("whale_alerts_scan: no tokens, exiting run")
        return

    now = time.time()
    loop = asyncio.get_event_loop()
    inserted = 0

    for t in tokens:
        addr = t["address"]
        symbol = t["symbol"]
        try:
            data = await loop.run_in_executor(None, _analyze_sync, addr, "eth")
            if not data.get("available"):
                continue

            # Always cache full result for endpoint pre-warm (upsert, replaces prev entry per token)
            try:
                verdict_pre = await _get_haiku_verdict(addr, data)
                data_with_verdict = dict(data)
                data_with_verdict["haiku_verdict"] = verdict_pre
                await db.whale_movements_cache.replace_one(
                    {"token_address": addr, "chain": "eth"},
                    {
                        "token_address": addr,
                        "chain": "eth",
                        "symbol": symbol,
                        "data": data_with_verdict,
                        "cached_at": now,
                    },
                    upsert=True,
                )
            except Exception as cache_exc:
                logger.warning(f"whale_alerts_scan: cache write failed for {symbol}: {cache_exc}")

            if not data.get("events"):
                continue

            verdict = verdict_pre  # reuse; skip second Haiku call
            risk = verdict.get("risk", "LOW")
            if risk not in ("MEDIUM", "HIGH"):
                continue

            # dedup: skip if we already alerted this token in the last hour
            recent = await db.whale_alerts.find_one({
                "token_address": addr,
                "created_at": {"$gt": now - DEDUP_WINDOW_SECONDS},
            })
            if recent:
                continue

            await db.whale_alerts.insert_one({
                "symbol": symbol,
                "token_address": addr,
                "risk": risk,
                "reasoning": verdict.get("reasoning", ""),
                "summary": data.get("summary", {}),
                "events": data.get("events", [])[:5],
                "created_at": now,
            })
            inserted += 1
            logger.info(f"whale_alerts_scan: ALERT saved for {symbol} risk={risk}")
        except Exception as exc:
            logger.warning(f"whale_alerts_scan: error checking {symbol}: {exc}")
            continue  # one bad token should never stop the batch

    logger.info(f"whale_alerts_scan: run complete, {inserted} alerts inserted")


@router.get("/feed")
async def whale_alerts_feed(limit: int = 30):
    db = _get_db()
    cursor = db.whale_alerts.find().sort("created_at", -1).limit(min(limit, 100))
    items = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        items.append(doc)
    return {"success": True, "data": items}
