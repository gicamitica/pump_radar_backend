"""
dune_enrichment.py - Post-enricher Dune on-chain validation layer.
NEW isolated module. Does NOT import/modify scanner.py, judge.py, enricher.py, snapshot.py.
Called after enricher, before Haiku judge. Adds factual on-chain metrics per candidate:
  - unique_buyers_1h / unique_sellers_1h (distinct wallets)
  - net_flow_usd_1h (buy volume - sell volume)
Currently ETH only (query 7954274 on dex.trades). Solana added later.
Cost: ~0.05 credits/token/scan. Failures are non-fatal (returns None fields).
"""
import os
import time
import logging
import requests
from typing import Dict, Any, Optional

logger = logging.getLogger("dune_enrichment")

DUNE_API_BASE = "https://api.dune.com/api/v1"
QUERY_BUYER_ANALYTICS_ETH = 7954274
POLL_TIMEOUT = 60
POLL_INTERVAL = 1.5

_cache: Dict[str, Any] = {}
CACHE_TTL = 600  # 10 min - un scan hourly nu re-cheama acelasi token


def _get_key() -> str:
    return os.environ.get("DUNE_API_KEY", "")


def _run_query(query_id: int, token: str, key: str) -> Optional[list]:
    try:
        r = requests.post(
            f"{DUNE_API_BASE}/query/{query_id}/execute",
            headers={"X-Dune-API-Key": key, "Content-Type": "application/json"},
            json={"query_parameters": {"token_address": token}},
            timeout=20,
        )
        exec_id = (r.json() or {}).get("execution_id")
        if not exec_id:
            return None
        deadline = time.time() + POLL_TIMEOUT
        while time.time() < deadline:
            s = requests.get(
                f"{DUNE_API_BASE}/execution/{exec_id}/status",
                headers={"X-Dune-API-Key": key},
                timeout=15,
            )
            state = (s.json() or {}).get("state", "")
            if state == "QUERY_STATE_COMPLETED":
                rr = requests.get(
                    f"{DUNE_API_BASE}/execution/{exec_id}/results",
                    headers={"X-Dune-API-Key": key},
                    timeout=15,
                )
                return (rr.json() or {}).get("result", {}).get("rows", [])
            if state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
                return None
            time.sleep(POLL_INTERVAL)
        return None
    except Exception as exc:
        logger.warning(f"dune_enrichment query failed for {token}: {exc}")
        return None


def get_onchain_validation(token_address: str, network: str) -> Dict[str, Any]:
    """Returns on-chain buyer analytics for a token. ETH only for now.
    Non-ETH or failure returns {'dune_validated': False} - non-fatal."""
    empty = {
        "dune_validated": False,
        "unique_buyers_1h": None,
        "unique_sellers_1h": None,
        "net_flow_usd_1h": None,
    }
    if (network or "").lower() not in ("eth", "ethereum"):
        return empty
    key = _get_key()
    if not key or not token_address:
        return empty

    cache_key = f"eth:{token_address.lower()}"
    now = time.time()
    hit = _cache.get(cache_key)
    if hit and now - hit[0] < CACHE_TTL:
        return hit[1]

    rows = _run_query(QUERY_BUYER_ANALYTICS_ETH, token_address, key)
    if not rows:
        return empty
    row = rows[0]
    result = {
        "dune_validated": True,
        "unique_buyers_1h": int(row.get("unique_buyers_1h") or 0),
        "unique_sellers_1h": int(row.get("unique_sellers_1h") or 0),
        "net_flow_usd_1h": round(float(row.get("net_flow_usd_1h") or 0), 2),
    }
    _cache[cache_key] = (now, result)
    return result


def enrich_batch(candidates: list) -> list:
    """Adds Dune on-chain validation fields to each candidate in-place.
    ETH candidates run in parallel (max 3 workers). Non-ETH get empty fields.
    Non-fatal: any failure leaves candidate with dune_validated=False."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    eth_cands = [c for c in candidates
                 if (c.get("network") or "").lower() in ("eth", "ethereum")
                 and c.get("token_address")]
    other_cands = [c for c in candidates if c not in eth_cands]

    # Non-ETH: empty fields imediat
    for c in other_cands:
        c.update({
            "dune_validated": False,
            "unique_buyers_1h": None,
            "unique_sellers_1h": None,
            "net_flow_usd_1h": None,
        })

    if not eth_cands:
        return candidates

    def _work(cand):
        return cand, get_onchain_validation(cand.get("token_address", ""), "eth")

    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = [ex.submit(_work, c) for c in eth_cands]
        for fut in as_completed(futures):
            try:
                cand, result = fut.result()
                cand.update(result)
            except Exception as exc:
                logger.warning(f"enrich_batch worker failed: {exc}")

    validated = sum(1 for c in eth_cands if c.get("dune_validated"))
    logger.info(f"dune_enrichment batch: {validated}/{len(eth_cands)} ETH candidates validated")
    return candidates
