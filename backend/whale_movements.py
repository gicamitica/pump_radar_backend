"""
whale_movements.py - Whale movement tracking via Dune Analytics (ETH + Solana) +
Haiku dump-risk judge. NEW module version, isolated. Does not import/modify
scanner.py, judge.py, enricher.py, snapshot.py.

v3 rewrite: replaced Etherscan-based single-chain (ETH-only) detection with
Dune Analytics queries covering both ETH and Solana through the same API,
per user request. Dune gives raw transfer movements (wallet, counterparty,
amount, time) but does NOT reliably price small/new tokens - so USD value is
computed separately via GeckoTerminal price lookup, same pattern as the
original ETH-only version.

Two Dune queries (both manually created and cost-tested, ~0.01-0.06 credits each):
- QUERY_TRANSFERS_ETH (7915016): tokens.transfers, last 1h
- QUERY_TRANSFERS_SOLANA (7914968): tokens_solana.transfers, last 1h
"""
import os
import time
import asyncio
import json as json_lib
from motor.motor_asyncio import AsyncIOMotorClient
_mongo_client = None
_MONGO_CACHE_TTL = 900
def _get_db():
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = AsyncIOMotorClient(os.environ["MONGO_URL"])
    return _mongo_client[os.environ["DB_NAME"]]

async def _require_pro_user(authorization):
    """Freemium gate: Whale Movements is a Pro feature.
    Blocks anonymous and free users with 402 Payment Required."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authentication required")
    token = authorization[7:]
    try:
        import jwt as _jwt
        payload = _jwt.decode(token, os.environ["JWT_SECRET"], algorithms=["HS256"])
        user_id = payload.get("sub")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token")
    db = _get_db()
    from bson import ObjectId
    try:
        user = await db.users.find_one({"_id": ObjectId(user_id)})
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid user")
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    sub = (user.get("subscription") or "free").lower()
    if sub == "free":
        raise HTTPException(status_code=402, detail="Whale Movements is a Pro feature")
    expiry = user.get("subscription_expiry")
    if expiry:
        if isinstance(expiry, str):
            expiry = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        if expiry < datetime.now(timezone.utc):
            raise HTTPException(status_code=402, detail="Subscription expired")
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import requests
from fastapi import APIRouter, HTTPException, Header

from funding_routes import KNOWN_CEX  # reuse existing ETH CEX address list

router = APIRouter(prefix="/api/crypto/whale-movements", tags=["whale-movements"])

DUNE_API_BASE = "https://api.dune.com/api/v1"
GECKOTERMINAL_BASE = "https://api.geckoterminal.com/api/v2"

QUERY_TRANSFERS_ETH = 7915016
QUERY_TRANSFERS_SOLANA = 7914968
QUERY_TRANSFERS_BASE = 8358608
QUERY_POOLS_ETH = 7934720

MIN_USD_THRESHOLD = 5_000
CACHE_TTL = 45  # 45s - aligned with frontend 120s auto-refresh
POLL_TIMEOUT = 120
POLL_INTERVAL = 1.5

_cache: Dict[str, Any] = {}
_consensus_cache: Dict[str, Any] = {}
_error_message_cache: Dict[str, str] = {}  # cached per error type, not per token


def _get_dune_key() -> str:
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


def _run_dune_query(query_id: int, token: str, key: str) -> Optional[list]:
    exec_id = _dune_execute(query_id, {"token_address": token}, key)
    if not exec_id:
        return None
    return _dune_poll_results(exec_id, key)


def _price_from_dexscreener(token: str) -> Optional[float]:
    """Fallback when GeckoTerminal is rate limited (429 per IP)."""
    try:
        r = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{token}", timeout=15)
        pairs = (r.json() or {}).get("pairs") or []
        for pair in pairs:
            price = pair.get("priceUsd")
            if price:
                return float(price)
    except Exception:
        pass
    return None


def _get_token_price_usd(token: str, chain: str) -> Optional[float]:
    network = ("solana" if chain == "solana"
               else "base" if chain == "base" else "eth")
    url = f"{GECKOTERMINAL_BASE}/networks/{network}/tokens/{token}"
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 429:
                time.sleep(2 * (attempt + 1))
                continue
            d = r.json()
            price = d.get("data", {}).get("attributes", {}).get("price_usd")
            if price is not None:
                return float(price)
            break
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    return _price_from_dexscreener(token)


def _get_token_pools(token: str, chain: str) -> Dict[str, str]:
    """Returns {pool_address_lowercase: dex_name}. Dune (ETH) with GeckoTerminal fallback."""
    pools: Dict[str, str] = {}
    # ETH: try Dune first (query 7934720 returns top pools by activity in last 24h)
    if chain == "eth":
        try:
            key = _get_dune_key()
            if key:
                rows = _run_dune_query(QUERY_POOLS_ETH, token, key) or []
                for row in rows:
                    addr = (row.get("project_contract_address") or "").lower()
                    if addr:
                        pools[addr] = "DEX pool"
                if pools:
                    return pools
        except Exception:
            pass
    # Fallback (Solana always, ETH if Dune failed): GeckoTerminal top 5
    network = ("solana" if chain == "solana"
               else "base" if chain == "base" else "eth")
    url = f"{GECKOTERMINAL_BASE}/networks/{network}/tokens/{token}/pools"
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 429:
                time.sleep(2 * (attempt + 1))
                continue
            d = r.json()
            for item in d.get("data", [])[:5]:
                attrs = item.get("attributes", {}) or {}
                addr = (attrs.get("address") or "").lower()
                if not addr:
                    continue
                dex_id = (
                    item.get("relationships", {})
                    .get("dex", {})
                    .get("data", {})
                    .get("id", "")
                )
                pools[addr] = dex_id or "DEX pool"
            break
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    if not pools:
        pools.update(_pools_from_token_endpoint(token, network))
    return pools


def _pools_from_token_endpoint(token: str, network: str) -> Dict[str, str]:
    """Last resort: relationships.top_pools from the token endpoint.
    One request instead of two, and it is the call we already make for price."""
    out: Dict[str, str] = {}
    try:
        r = requests.get(f"{GECKOTERMINAL_BASE}/networks/{network}/tokens/{token}", timeout=15)
        if r.status_code != 200:
            return out
        rel = (r.json() or {}).get("data", {}).get("relationships", {}) or {}
        for item in (rel.get("top_pools", {}) or {}).get("data", [])[:5]:
            pid = item.get("id") or ""
            addr = pid.split("_", 1)[1].lower() if "_" in pid else ""
            if addr:
                out[addr] = "DEX pool"
    except Exception:
        pass
    return out


def _classify(wallet: str, counterparty: str, chain: str, pools: Dict[str, str]) -> Optional[tuple]:
    """Returns (direction, label, whale_address) or None.

    Checks BOTH directions: a private wallet sending TO a known CEX/pool
    (selling) or a known CEX/pool sending TO a private wallet (buying/
    withdrawing). Whichever side is NOT infrastructure is the actual whale
    we report on - it isn't always `wallet` (the row's sender)."""
    w = wallet.lower()
    cp = counterparty.lower()

    if chain in ("eth", "base") and cp in KNOWN_CEX:
        return "TO_EXCHANGE", KNOWN_CEX[cp], w
    if cp in pools:
        return "SWAP", pools[cp], w
    if chain in ("eth", "base") and w in KNOWN_CEX:
        return "WITHDRAWAL", f"from {KNOWN_CEX[w]}", cp
    if w in pools:
        return "WITHDRAWAL", f"from {pools[w]}", cp
    return None  # both sides private/unknown - not meaningful noise


async def _get_error_explanation(error_code: str) -> str:
    """Cached per error type (not per token) - one Haiku call covers all
    tokens hitting the same failure reason."""
    if error_code in _error_message_cache:
        return _error_message_cache[error_code]

    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    fallback = {
        "no_dune_key": "Whale tracking is temporarily unavailable due to a configuration issue.",
        "no_price_data": "No live price data was found for this token, so USD values for whale movements can't be calculated. This is common for very new or low-liquidity tokens.",
        "dune_query_failed": "Whale movement data couldn't be retrieved right now. This can happen during high query load - try again shortly.",
        "no_transfers": "No wallet transfers were found for this token in the last hour.",
    }.get(error_code, "Whale movement data isn't available for this token right now.")

    if not api_key:
        _error_message_cache[error_code] = fallback
        return fallback

    try:
        def _request():
            return requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 60,
                    "temperature": 0.0,
                    "system": (
                        "Write ONE short, plain-English sentence (max 25 words) explaining to a "
                        "crypto trader why whale movement data is unavailable, given this internal "
                        "error code. No markdown, no preamble, just the sentence."
                    ),
                    "messages": [{"role": "user", "content": f"error_code: {error_code}"}],
                },
                timeout=15,
            )
        resp = await asyncio.to_thread(_request)
        if resp.status_code >= 400:
            _error_message_cache[error_code] = fallback
            return fallback
        d = resp.json() or {}
        text = ((d.get("content") or [{}])[0].get("text") or "").strip()
        msg = text if text else fallback
        _error_message_cache[error_code] = msg
        return msg
    except Exception:
        _error_message_cache[error_code] = fallback
        return fallback


def _parse_dune_timestamp(block_time_str: str) -> float:
    """Dune returns block_time as a string like '2026-07-08 10:14:59.000 UTC'
    (Etherscan gave us a Unix int before the Dune rewrite). Convert to Unix
    epoch seconds so the frontend's numeric time-ago math doesn't get NaN."""
    if not block_time_str:
        return 0.0
    s = block_time_str.strip()
    if s.endswith(" UTC"):
        s = s[:-4].strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            continue
    return 0.0


def _analyze_sync(token: str, chain: str) -> Dict[str, Any]:
    key = _get_dune_key()
    if not key:
        return {"available": False, "error": "no_dune_key"}

    query_id = (QUERY_TRANSFERS_ETH if chain == "eth"
                else QUERY_TRANSFERS_BASE if chain == "base"
                else QUERY_TRANSFERS_SOLANA)
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=3) as ex:
        fut_rows = ex.submit(_run_dune_query, query_id, token, key)
        fut_price = ex.submit(_get_token_price_usd, token, chain)
        fut_pools = ex.submit(_get_token_pools, token, chain)
        rows = fut_rows.result()
        price = fut_price.result()
        pools = fut_pools.result()

    if rows is None:
        return {"available": False, "error": "dune_query_failed"}
    if not price:
        return {"available": False, "error": "no_price_data"}

    events: List[dict] = []
    for row in rows:
        amount = float(row.get("amount") or 0)
        amount_usd = amount * price
        if amount_usd < MIN_USD_THRESHOLD:
            continue
        wallet = (row.get("wallet") or "").lower()
        counterparty = (row.get("counterparty") or "").lower()
        classified = _classify(wallet, counterparty, chain, pools)
        if not classified:
            continue
        direction, label, whale_address = classified
        events.append({
            "wallet": whale_address,
            "amount_usd": round(amount_usd, 2),
            "direction": direction,
            "destination_label": label,
            "timestamp": _parse_dune_timestamp(row.get("block_time", "")),
        })

    total_to_exchange = sum(e["amount_usd"] for e in events if e["direction"] == "TO_EXCHANGE")
    total_swap = sum(e["amount_usd"] for e in events if e["direction"] == "SWAP")
    total_withdrawn = sum(e["amount_usd"] for e in events if e["direction"] == "WITHDRAWAL")
    whales_selling = len({e["wallet"] for e in events if e["direction"] in ("TO_EXCHANGE", "SWAP")})
    whales_buying = len({e["wallet"] for e in events if e["direction"] == "WITHDRAWAL"})

    return {
        "available": True,
        "chain": chain,
        "window_hours": 1,
        "min_usd_threshold": MIN_USD_THRESHOLD,
        "rows_scanned": len(rows),
        "pools_tracked": len(pools),
        "events": events[:20],
        "summary": {
            "total_to_exchange_usd": round(total_to_exchange, 2),
            "total_swap_usd": round(total_swap, 2),
            "total_withdrawn_usd": round(total_withdrawn, 2),
            "net_pressure_usd": round(total_to_exchange + total_swap - total_withdrawn, 2),
            "whales_selling_or_moving_out": whales_selling,
            "whales_withdrawing": whales_buying,
        },
    }


async def _get_haiku_verdict(token: str, data: Dict[str, Any]) -> Dict[str, Any]:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return {"risk": "UNKNOWN", "reasoning": "no_api_key"}
    # empty state: cerem tot Haiku sa dea un mesaj contextual, nu placeholder

    system_instruction = (
        "You are an on-chain analyst judging short-term dump risk from whale movement data. "
        "Respond ONLY with JSON: {\"risk\": \"LOW|MEDIUM|HIGH\", \"reasoning\": \"1-2 sentences\"}. "
        "No markdown, no preamble."
    )
    user_prompt = (
        f"Token: {token} (chain: {data.get('chain')})\n"
        f"Window: last {data['window_hours']}h, threshold ${data['min_usd_threshold']}\n"
        f"Summary: {json_lib.dumps(data['summary'])}\n"
        f"Events (most recent first): {json_lib.dumps(data['events'][:10])}\n\n"
        "Consider how many distinct whales are moving toward exchanges/swaps and the net "
        "USD pressure. Decide dump risk."
    )

    try:
        def _request():
            return requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 150,
                    "temperature": 0.0,
                    "system": system_instruction,
                    "messages": [{"role": "user", "content": user_prompt}],
                },
                timeout=30,
            )
        resp = await asyncio.to_thread(_request)
        if resp.status_code >= 400:
            return {"risk": "UNKNOWN", "reasoning": f"haiku_error_{resp.status_code}"}
        d = resp.json() or {}
        text = ((d.get("content") or [{}])[0].get("text") or "").strip()
        if text.startswith("```"):
            text = "\n".join(l for l in text.splitlines() if not l.strip().startswith("```")).strip()
        parsed = json_lib.loads(text)
        return {"risk": parsed.get("risk", "UNKNOWN"), "reasoning": parsed.get("reasoning", "")}
    except Exception as exc:
        return {"risk": "UNKNOWN", "reasoning": f"error: {str(exc)[:150]}"}


async def _analyze(token: str, chain: str) -> Dict[str, Any]:
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, _analyze_sync, token, chain)
    if not data.get("available"):
        data["user_message"] = await _get_error_explanation(data.get("error", "unknown"))
        return data
    verdict = await _get_haiku_verdict(token, data)
    data["haiku_verdict"] = verdict
    return data


@router.get("/eth/{address}")
async def whale_movements_eth(address: str, authorization=Header(None)):
    await _require_pro_user(authorization)
    address = address.lower().strip()
    if not (address.startswith("0x") and len(address) == 42):
        raise HTTPException(status_code=400, detail="invalid eth address")

    now = time.time()
    cache_key = f"eth:{address}"
    # Level 1: in-memory cache (45s TTL)
    hit = _cache.get(cache_key)
    if hit and now - hit[0] < CACHE_TTL:
        return {"success": True, "cached": True, "data": hit[1]}

    # Level 2: MongoDB pre-warmed cache (from whale_alerts_job hourly)
    try:
        db = _get_db()
        doc = await db.whale_movements_cache.find_one({"token_address": address, "chain": "eth"})
        if doc and (now - doc.get("cached_at", 0) < _MONGO_CACHE_TTL):
            cached_data = doc.get("data", {})
            _cache[cache_key] = (now, cached_data)
            return {"success": True, "cached": True, "cache_source": "mongo_prewarm", "data": cached_data}
    except Exception:
        pass

    # Level 3: live Dune fetch (slow)
    data = await _analyze(address, "eth")
    if data.get("available"):
        _cache[cache_key] = (now, data)
    return {"success": True, "cached": False, "data": data}


@router.get("/base/{address}")
async def whale_movements_base(address: str, authorization=Header(None)):
    await _require_pro_user(authorization)
    address = address.lower().strip()
    if not (address.startswith("0x") and len(address) == 42):
        raise HTTPException(status_code=400, detail="invalid base address")

    now = time.time()
    cache_key = f"base:{address}"
    hit = _cache.get(cache_key)
    if hit and now - hit[0] < CACHE_TTL:
        return {"success": True, "cached": True, "data": hit[1]}

    try:
        db = _get_db()
        doc = await db.whale_movements_cache.find_one({"token_address": address, "chain": "base"})
        if doc and (now - doc.get("cached_at", 0) < _MONGO_CACHE_TTL):
            cached_data = doc.get("data", {})
            _cache[cache_key] = (now, cached_data)
            return {"success": True, "cached": True, "cache_source": "mongo_prewarm", "data": cached_data}
    except Exception:
        pass

    data = await _analyze(address, "base")
    if data.get("available"):
        _cache[cache_key] = (now, data)
    return {"success": True, "cached": False, "data": data}


@router.get("/solana/{address}")
async def whale_movements_solana(address: str, authorization=Header(None)):
    await _require_pro_user(authorization)
    address = address.strip()
    if not address or len(address) < 32:
        raise HTTPException(status_code=400, detail="invalid solana address")

    now = time.time()
    cache_key = f"solana:{address}"
    hit = _cache.get(cache_key)
    if hit and now - hit[0] < CACHE_TTL:
        return {"success": True, "cached": True, "data": hit[1]}

    data = await _analyze(address, "solana")
    if data.get("available"):
        _cache[cache_key] = (now, data)
    return {"success": True, "cached": False, "data": data}


# -------- CONSENSUS ENDPOINT --------
async def _get_consensus_verdict(token: str, whale_data: dict, dune_data: dict) -> str:
    """Haiku syntetizeaza cele 2 carduri intr-o propozitie pt banner."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return "Consensus unavailable."
    whale_summary = whale_data.get("summary", {}) if whale_data.get("available") else {}
    whale_events = len(whale_data.get("events", [])) if whale_data.get("available") else 0
    system_instruction = (
        "You are a crypto analyst. Given whale movement data (>$5k, 1h) and market volume data "
        "(all traders, 1h), write ONE short sentence (max 20 words) synthesizing the 1h consensus. "
        "Format: 'whales <state> · retail <state>'. No JSON, no markdown, plain text only."
    )
    user_prompt = (
        f"Token: {token}\n"
        f"Whale movements 1h (>=$5k): {whale_events} events, summary={json_lib.dumps(whale_summary)}\n"
        f"Market volume 1h (all sizes): {json_lib.dumps(dune_data) if dune_data else 'no_data'}\n"
    )
    try:
        def _request():
            return requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 60,
                    "temperature": 0.0,
                    "system": system_instruction,
                    "messages": [{"role": "user", "content": user_prompt}],
                },
                timeout=20,
            )
        resp = await asyncio.to_thread(_request)
        if resp.status_code >= 400:
            return "Consensus unavailable."
        d = resp.json() or {}
        text = ((d.get("content") or [{}])[0].get("text") or "").strip()
        return text or "Consensus unavailable."
    except Exception:
        return "Consensus unavailable."


@router.get("/consensus/eth/{address}")
async def whale_consensus_eth(address: str, authorization=Header(None)):
    await _require_pro_user(authorization)
    now = time.time()
    cache_key = f"consensus_eth_{address.lower()}"
    hit = _consensus_cache.get(cache_key)
    if hit and now - hit[0] < CACHE_TTL:
        return {"success": True, "cached": True, "data": hit[1]}
    whale_data = await _analyze(address, "eth")
    try:
        from dune_routes import _analyze_sync as dune_analyze_sync
        loop = asyncio.get_event_loop()
        dune_data = await loop.run_in_executor(None, dune_analyze_sync, address, "eth")
    except Exception as exc:
        dune_data = {"error": str(exc)[:100]}
    verdict = await _get_consensus_verdict(address, whale_data, dune_data)
    data = {"consensus": verdict, "chain": "eth"}
    _consensus_cache[cache_key] = (now, data)
    return {"success": True, "cached": False, "data": data}

