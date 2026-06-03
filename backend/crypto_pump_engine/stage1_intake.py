from __future__ import annotations

from typing import Any, Dict, List, Optional

import httpx

from .config import CONFIG, looks_like_quote_symbol, normalize_chain
from .models import IntakeResult, PairTokenRef


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _pick_best_pair(pairs: List[Dict[str, Any]], pair_address: str) -> Optional[Dict[str, Any]]:
    if not pairs:
        return None
    lowered_pair_address = (pair_address or "").lower()
    for pair in pairs:
        if (pair.get("pairAddress") or "").lower() == lowered_pair_address:
            return pair
    return max(
        pairs,
        key=lambda item: (
            _as_float((item.get("liquidity") or {}).get("usd")),
            _as_float((item.get("volume") or {}).get("h24")),
        ),
    )


def _pick_top_pair(pairs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not pairs:
        return None
    return max(
        pairs,
        key=lambda item: (
            _as_float((item.get("liquidity") or {}).get("usd")),
            _as_float((item.get("volume") or {}).get("h24")),
        ),
    )


def _resolve_token_address(pair: Dict[str, Any], explicit_token_address: Optional[str]) -> str:
    if explicit_token_address:
        return explicit_token_address

    base_token = pair.get("baseToken") or {}
    quote_token = pair.get("quoteToken") or {}
    base_symbol = base_token.get("symbol") or ""
    quote_symbol = quote_token.get("symbol") or ""

    if looks_like_quote_symbol(quote_symbol) and base_token.get("address"):
        return base_token["address"]
    if looks_like_quote_symbol(base_symbol) and quote_token.get("address"):
        return quote_token["address"]
    return base_token.get("address") or quote_token.get("address") or ""


async def _fetch_json(client: httpx.AsyncClient, url: str) -> Any:
    response = await client.get(url)
    response.raise_for_status()
    return response.json()


async def resolve_pair_for_token(
    client: httpx.AsyncClient,
    chain: str,
    token_address: str,
) -> Optional[Dict[str, Any]]:
    normalized_chain = normalize_chain(chain)
    ### LIVE API ###
    payload = await _fetch_json(
        client,
        f"{CONFIG['dexscreener_base_url']}/token-pairs/v1/{normalized_chain}/{token_address}",
    )
    if not isinstance(payload, list):
        return None
    return _pick_top_pair(payload)


async def run_stage(
    client: httpx.AsyncClient,
    chain: str,
    pair_address: str,
    token_address: Optional[str] = None,
) -> IntakeResult:
    normalized_chain = normalize_chain(chain)
    pair_url = f"{CONFIG['dexscreener_base_url']}/latest/dex/pairs/{normalized_chain}/{pair_address}"

    ### LIVE API ###
    pair_payload = await _fetch_json(client, pair_url)
    pairs = pair_payload.get("pairs") or []
    pair = _pick_best_pair(pairs, pair_address)
    if not pair:
        raise ValueError(f"Pair not found on DexScreener for chain={normalized_chain} pair={pair_address}")

    resolved_token_address = _resolve_token_address(pair, token_address)
    if not resolved_token_address:
        raise ValueError("Unable to resolve token_address from pair details")

    ### LIVE API ###
    orders_url = f"{CONFIG['dexscreener_base_url']}/orders/v1/{normalized_chain}/{resolved_token_address}"
    try:
        orders_payload = await _fetch_json(client, orders_url)
    except Exception:
        orders_payload = []

    txns_h24 = ((pair.get("txns") or {}).get("h24") or {})
    info = pair.get("info") or {}
    websites = [item.get("url") for item in (info.get("websites") or []) if item.get("url")]
    socials = [
        {"platform": item.get("platform", ""), "handle": item.get("handle", "")}
        for item in (info.get("socials") or [])
        if item.get("platform") or item.get("handle")
    ]

    intake = IntakeResult(
        chain=normalized_chain,
        pair_address=pair.get("pairAddress") or pair_address,
        token_address=resolved_token_address,
        candidate_id=f"{normalized_chain}_{pair.get('pairAddress') or pair_address}",
        pair_url=pair.get("url"),
        dex_id=pair.get("dexId"),
        base_token=PairTokenRef(**(pair.get("baseToken") or {})),
        quote_token=PairTokenRef(**(pair.get("quoteToken") or {})),
        price_usd=_as_float(pair.get("priceUsd"), None),
        liquidity_usd=_as_float((pair.get("liquidity") or {}).get("usd")),
        volume_24h_usd=_as_float((pair.get("volume") or {}).get("h24")),
        buys_24h=_as_int(txns_h24.get("buys")),
        sells_24h=_as_int(txns_h24.get("sells")),
        price_change_24h_pct=_as_float((pair.get("priceChange") or {}).get("h24")),
        pair_created_at_ms=_as_int(pair.get("pairCreatedAt"), None),
        boost_count=_as_int((pair.get("boosts") or {}).get("active")),
        paid_order_count=len(orders_payload) if isinstance(orders_payload, list) else 0,
        profile_links=(info.get("socials") or []),
        websites=websites,
        socials=socials,
        labels=[label for label in (pair.get("labels") or []) if label],
        raw_pair=pair,
        stage_mode="live",
        risks=[],
    )

    if not token_address:
        intake.risks.append("token_address_inferred_from_pair")

    return intake
