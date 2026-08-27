"""
dune_routes.py - Market-wide buyer/seller volume + wash trading detection via Dune Analytics.
NEW module, isolated. Does not import/modify scanner.py, judge.py, enricher.py, snapshot.py.

v2: extended to cover both ETH and Solana, per user request. Solana queries use
a 12h window (not 1h like ETH) because dex_solana.trades has several hours of
indexing lag - a 1h window would always return empty regardless of real activity.
Also: on dex_solana.trades, combining both address columns with OR in a single
WHERE clause caused a silent-empty-result + high-cost query (30+ credits, 0 rows)
on this large table - fixed by using separate sub-queries / UNION ALL instead.

Four Dune queries total, all manually created and cost-tested:
- QUERY_BUYER_SELLER_ETH (7904729): dex.trades, 1h window, ~0.005-0.05 credits
- QUERY_WASH_TRADING_ETH (7904779): dex.trades, 1h window, ~0.02-0.12 credits
- QUERY_BUYER_SELLER_SOLANA (7915441): dex_solana.trades, 12h window, ~0.01-0.02 credits
- QUERY_WASH_TRADING_SOLANA (7915616): dex_solana.trades, 12h window, ~0.07 credits
"""
import os
import time
import asyncio
from typing import Dict, Any, Optional

import requests
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/crypto/dune", tags=["dune"])

DUNE_API_BASE = "https://api.dune.com/api/v1"

QUERY_BUYER_SELLER_ETH = 7904729
QUERY_WASH_TRADING_ETH = 7904779
QUERY_BUYER_SELLER_SOLANA = 7915441
QUERY_WASH_TRADING_SOLANA = 7915616

CACHE_TTL = 900  # 15 min
POLL_TIMEOUT = 60
POLL_INTERVAL = 1.5

_cache: Dict[str, Any] = {}


def _get_key() -> str:
    return os.environ.get("DUNE_API_KEY", "")


def _dune_execute(query_id: int, params: dict, key: str) -> Optional[str]:
    try:
        r = requests.post(
            f"{DUNE_API_BASE}/query/{query_id}/execute",
            headers={"X-Dune-API-Key": key, "Content-Type": "application/json"},
            json={"query_parameters": params},
            timeout=20,
        )
        if r.status_code >= 400:
            return None
        return r.json().get("execution_id")
    except Exception:
        return None


def _dune_poll_results(execution_id: str, key: str) -> Optional[list]:
    deadline = time.time() + POLL_TIMEOUT
    while time.time() < deadline:
        try:
            r = requests.get(
                f"{DUNE_API_BASE}/execution/{execution_id}/status",
                headers={"X-Dune-API-Key": key},
                timeout=15,
            )
            state = r.json().get("state", "")
            if state == "QUERY_STATE_COMPLETED":
                rr = requests.get(
                    f"{DUNE_API_BASE}/execution/{execution_id}/results",
                    headers={"X-Dune-API-Key": key},
                    timeout=15,
                )
                return rr.json().get("result", {}).get("rows", [])
            if state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
                return None
        except Exception:
            pass
        time.sleep(POLL_INTERVAL)
    return None


def _run_query(query_id: int, token: str, key: str) -> Optional[list]:
    exec_id = _dune_execute(query_id, {"token_address": token}, key)
    if not exec_id:
        return None
    return _dune_poll_results(exec_id, key)


def _analyze_sync(token: str, chain: str) -> Dict[str, Any]:
    key = _get_key()
    if not key:
        return {"available": False, "error": "no_dune_key"}

    is_solana = chain == "solana"
    window_hours = 12 if is_solana else 1
    bs_query = QUERY_BUYER_SELLER_SOLANA if is_solana else QUERY_BUYER_SELLER_ETH
    wash_query = QUERY_WASH_TRADING_SOLANA if is_solana else QUERY_WASH_TRADING_ETH

    bs_rows = _run_query(bs_query, token, key)
    if bs_rows is None:
        return {"available": False, "error": "buyer_seller_query_failed"}

    wash_rows = _run_query(wash_query, token, key)
    if wash_rows is None:
        wash_rows = []  # non-fatal: wash trading is a bonus signal

    bs = bs_rows[0] if bs_rows else {}
    buyer_vol = float(bs.get("buyer_volume_usd") or 0)
    seller_vol = float(bs.get("seller_volume_usd") or 0)

    wash_total = sum(float(r.get("round_trip_volume_usd") or 0) for r in wash_rows)
    wash_wallets = len(wash_rows)
    top_wash = sorted(wash_rows, key=lambda r: -float(r.get("round_trip_volume_usd") or 0))[:5]

    total_vol = buyer_vol + seller_vol
    wash_ratio = (wash_total / total_vol) if total_vol > 0 else 0.0
    wash_ratio_pct = round(wash_ratio * 100, 2)

    if wash_ratio_pct < 15:
        wash_risk = "LOW"
    elif wash_ratio_pct <= 40:
        wash_risk = "MEDIUM"
    else:
        wash_risk = "HIGH"

    return {
        "available": True,
        "chain": chain,
        "window_hours": window_hours,
        "buyer_volume_usd": round(buyer_vol, 2),
        "seller_volume_usd": round(seller_vol, 2),
        "wash_trading": {
            "total_round_trip_volume_usd": round(wash_total, 2),
            "distinct_wash_wallets": wash_wallets,
            "wash_ratio_pct": wash_ratio_pct,
            "wash_risk": wash_risk,
            "top_wallets": [
                {
                    "wallet": w.get("taker", ""),
                    "round_trip_count": int(w.get("round_trip_count") or 0),
                    "volume_usd": round(float(w.get("round_trip_volume_usd") or 0), 2),
                }
                for w in top_wash
            ],
        },
    }


@router.get("/market-signals/eth/{address}")
async def dune_market_signals_eth(address: str):
    address = address.lower().strip()
    if not (address.startswith("0x") and len(address) == 42):
        raise HTTPException(status_code=400, detail="invalid eth address")

    now = time.time()
    cache_key = f"eth:{address}"
    hit = _cache.get(cache_key)
    if hit and now - hit[0] < CACHE_TTL:
        return {"success": True, "cached": True, "data": hit[1]}

    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, _analyze_sync, address, "eth")

    if data.get("available"):
        _cache[cache_key] = (now, data)

    return {"success": True, "cached": False, "data": data}


@router.get("/market-signals/solana/{address}")
async def dune_market_signals_solana(address: str):
    address = address.strip()
    if not address or len(address) < 32:
        raise HTTPException(status_code=400, detail="invalid solana address")

    now = time.time()
    cache_key = f"solana:{address}"
    hit = _cache.get(cache_key)
    if hit and now - hit[0] < CACHE_TTL:
        return {"success": True, "cached": True, "data": hit[1]}

    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, _analyze_sync, address, "solana")

    if data.get("available"):
        _cache[cache_key] = (now, data)

    return {"success": True, "cached": False, "data": data}
