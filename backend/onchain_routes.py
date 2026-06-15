"""
onchain_routes.py - read-only API layer for the on-chain intelligence module.

NEW module. Does NOT touch scanner.py / judge.py / enricher.py / classification.
Reads only from the `onchain_events` collection. The ingestion service and the
enrichment worker (separate processes) are the only writers.

Wiring in server.py (single line block, after `app` and your Mongo `db` exist):

    from onchain_routes import router as onchain_router, set_db
    set_db(db)                       # pass your existing AsyncIOMotorDatabase
    app.include_router(onchain_router)

Assumes an async (motor) db handle. If server.py uses sync pymongo instead,
say so and a sync variant will be provided.
"""
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

from fastapi import APIRouter, Query, HTTPException

router = APIRouter(prefix="/api/crypto/onchain", tags=["onchain"])

_db = None  # set via set_db() from server.py


def set_db(db) -> None:
    global _db
    _db = db


def _coll():
    if _db is None:
        raise HTTPException(status_code=503, detail="onchain db not initialized")
    return _db["onchain_events"]


VALID_CHAINS = {"eth", "bsc"}
LP_EVENTS = ["lp_add", "lp_lock", "lp_remove"]


def _chain_filter(chain: Optional[str]) -> Dict[str, Any]:
    if chain in (None, "", "all"):
        return {}
    c = chain.lower()
    if c not in VALID_CHAINS:
        raise HTTPException(status_code=400, detail=f"unsupported chain: {chain}")
    return {"chain": c}


def _require_chain(chain: str) -> str:
    c = chain.lower()
    if c not in VALID_CHAINS:
        raise HTTPException(status_code=400, detail=f"unsupported chain: {chain}")
    return c


def _since_filter(minutes: Optional[int]) -> Dict[str, Any]:
    if not minutes:
        return {}
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    return {"block_time": {"$gte": cutoff}}


async def _find(query, sort, limit):
    cur = _coll().find(query, {"_id": 0}).sort(sort).limit(limit)
    return [d async for d in cur]


@router.get("/new-pairs")
async def new_pairs(
    chain: str = Query("all"),
    since_minutes: int = Query(720, ge=1, le=20160),
    min_liq_usd: float = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
):
    q = {"event_type": "pair_created"}
    q.update(_chain_filter(chain))
    q.update(_since_filter(since_minutes))
    if min_liq_usd > 0:
        q["enrichment.liquidity.initial_usd"] = {"$gte": min_liq_usd}
    rows = await _find(q, [("block_time", -1)], limit)
    return {"count": len(rows), "chain": chain, "events": rows}


@router.get("/early")
async def early_feed(
    chain: str = Query("all"),
    min_score: int = Query(60, ge=0, le=100),
    since_minutes: int = Query(720, ge=1, le=20160),
    limit: int = Query(50, ge=1, le=200),
):
    q = {"scores.early.score": {"$gte": min_score}}
    q.update(_chain_filter(chain))
    q.update(_since_filter(since_minutes))
    rows = await _find(q, [("scores.early.score", -1), ("block_time", -1)], limit)
    return {"count": len(rows), "events": rows}


@router.get("/threat")
async def threat_feed(
    chain: str = Query("all"),
    min_score: int = Query(50, ge=0, le=100),
    label: Optional[str] = Query(None),
    since_minutes: int = Query(1440, ge=1, le=43200),
    limit: int = Query(50, ge=1, le=200),
):
    q = {"scores.threat.score": {"$gte": min_score}}
    q.update(_chain_filter(chain))
    q.update(_since_filter(since_minutes))
    if label:
        q["scores.threat.label"] = label
    rows = await _find(q, [("scores.threat.score", -1), ("block_time", -1)], limit)
    return {"count": len(rows), "events": rows}


@router.get("/token/{chain}/{address}")
async def token_profile(chain: str, address: str):
    c = _require_chain(chain)
    addr = address.lower()
    q = {"chain": c, "token_address": addr}
    events = await _find(q, [("block_time", 1)], 500)
    if not events:
        raise HTTPException(status_code=404, detail="no on-chain events for token")
    latest = next((e for e in reversed(events) if e.get("enriched")), events[-1])
    return {
        "chain": c,
        "token_address": addr,
        "event_count": len(events),
        "enrichment": latest.get("enrichment"),
        "scores": latest.get("scores"),
        "events": events,
    }


@router.get("/deployer/{chain}/{address}")
async def deployer_lineage(chain: str, address: str):
    c = _require_chain(chain)
    addr = address.lower()
    q = {"chain": c, "enrichment.deployer.address": addr}
    rows = await _find(q, [("block_time", -1)], 500)
    tokens: Dict[str, Any] = {}
    for r in rows:
        t = r.get("token_address")
        if not t or t in tokens:
            continue
        sc = r.get("scores") or {}
        th = sc.get("threat") or {}
        tokens[t] = {
            "token_address": t,
            "first_seen": r.get("block_time"),
            "threat_label": th.get("label"),
        }
    rug_count = sum(
        1 for t in tokens.values()
        if t["threat_label"] in ("scam_likely", "confirmed_rug")
    )
    return {
        "chain": c,
        "deployer": addr,
        "token_count": len(tokens),
        "rug_count": rug_count,
        "tokens": list(tokens.values()),
    }


@router.get("/lp-events/{chain}/{pair_address}")
async def lp_events(chain: str, pair_address: str):
    c = _require_chain(chain)
    pair = pair_address.lower()
    q = {"chain": c, "pair_address": pair, "event_type": {"$in": LP_EVENTS}}
    rows = await _find(q, [("block_time", 1)], 500)
    return {
        "chain": c,
        "pair_address": pair,
        "lp_locked": any(r["event_type"] == "lp_lock" for r in rows),
        "lp_removed": any(r["event_type"] == "lp_remove" for r in rows),
        "timeline": rows,
    }


@router.get("/stats")
async def stats():
    coll = _coll()
    out: Dict[str, Any] = {}
    for c in sorted(VALID_CHAINS):
        total = await coll.count_documents({"chain": c})
        last = await coll.find_one(
            {"chain": c},
            {"_id": 0, "block_number": 1, "block_time": 1},
            sort=[("block_number", -1)],
        )
        pending = await coll.count_documents({"chain": c, "enriched": {"$ne": True}})
        out[c] = {
            "total_events": total,
            "last_block": (last or {}).get("block_number"),
            "last_block_time": (last or {}).get("block_time"),
            "pending_enrichment": pending,
        }
    return {"chains": out, "generated_at": datetime.now(timezone.utc)}
