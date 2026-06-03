from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from .config import CONFIG, get_honeypot_chain_id
from .models import HoldersResult, IntakeResult


def _as_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


async def _fetch_json(client: httpx.AsyncClient, url: str, params: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> Any:
    response = await client.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()


def _heuristic_holders(intake: IntakeResult) -> HoldersResult:
    ### MOCK (replace later) ###
    ### HEURISTIC ###
    concentration = 0.38
    if intake.liquidity_usd >= 50000:
        concentration = 0.18
    elif intake.liquidity_usd >= 15000:
        concentration = 0.26

    risks = ["holders_mock_fallback"]
    if concentration >= 0.35:
        risks.append("holder_concentration_high")

    return HoldersResult(
        concentration=round(concentration, 4),
        top_1_share=round(concentration * 0.45, 4),
        top_5_share=round(concentration * 0.75, 4),
        top_10_share=round(min(0.98, concentration * 1.1), 4),
        holder_count_sample=0,
        source_mode="mock",
        risks=sorted(set(risks)),
    )


async def run_stage(client: httpx.AsyncClient, intake: IntakeResult) -> HoldersResult:
    chain_id = get_honeypot_chain_id(intake.chain)
    if not chain_id:
        return _heuristic_holders(intake)

    headers = {"X-API-KEY": CONFIG["honeypot_api_key"]} if CONFIG["honeypot_api_key"] else None

    try:
        ### LIVE API ###
        payload = await _fetch_json(
            client,
            f"{CONFIG['honeypot_base_url']}/v1/TopHolders",
            params={"address": intake.token_address, "chainID": chain_id},
            headers=headers,
        )
        total_supply = _as_float(payload.get("totalSupply"), 0.0) or 0.0
        holders = payload.get("holders") or []
        if total_supply <= 0 or not holders:
            return _heuristic_holders(intake)

        shares = []
        for holder in holders:
            balance = _as_float(holder.get("balance"), 0.0) or 0.0
            shares.append(balance / total_supply if total_supply else 0.0)

        top_1_share = sum(shares[:1])
        top_5_share = sum(shares[:5])
        top_10_share = sum(shares[:10])
        concentration = top_10_share

        risks = []
        if top_1_share >= 0.2:
            risks.append("single_holder_dominance")
        if concentration >= 0.35:
            risks.append("holder_concentration_high")

        return HoldersResult(
            concentration=round(concentration, 4),
            top_1_share=round(top_1_share, 4),
            top_5_share=round(top_5_share, 4),
            top_10_share=round(top_10_share, 4),
            holder_count_sample=len(holders),
            source_mode="live",
            risks=sorted(set(risks)),
        )
    except Exception:
        return _heuristic_holders(intake)
