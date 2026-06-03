from __future__ import annotations

from statistics import mean
from typing import Any, Dict, List, Optional

import httpx

from .config import CONFIG, get_goplus_chain_id, get_honeypot_chain_id, get_tokensniffer_chain_id
from .models import IntakeResult, SafetyProviderResult, SafetyResult


def _as_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _flag_enabled(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def _round_score(value: float) -> float:
    return max(0.0, min(100.0, round(value, 2)))


async def _fetch_json(client: httpx.AsyncClient, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Any:
    response = await client.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()


async def _run_goplus(client: httpx.AsyncClient, intake: IntakeResult) -> SafetyProviderResult:
    chain_id = get_goplus_chain_id(intake.chain)
    if not chain_id:
        return SafetyProviderResult(provider="goplus", available=False, risks=["goplus_unsupported_chain"])

    ### LIVE API ###
    url = f"{CONFIG['goplus_base_url']}/api/v1/token_security/{chain_id}"
    payload = await _fetch_json(client, url, params={"contract_addresses": intake.token_address})
    token_map = payload.get("result") or {}
    token_data = token_map.get(intake.token_address) or token_map.get(intake.token_address.lower()) or {}
    if not token_data:
        return SafetyProviderResult(provider="goplus", available=False, risks=["goplus_empty_result"])

    score = 65.0
    risks: List[str] = []
    hard_veto = False

    if _flag_enabled(token_data.get("is_open_source")):
        score += 12
    else:
        score -= 20
        risks.append("contract_not_open_source")

    if _flag_enabled(token_data.get("is_proxy")):
        score -= 6
        risks.append("proxy_contract")

    if _flag_enabled(token_data.get("malicious_address")):
        score -= 40
        risks.append("malicious_address_flag")
        hard_veto = True

    if _flag_enabled(token_data.get("honeypot_with_same_creator")):
        score -= 20
        risks.append("honeypot_related_creator")

    if _flag_enabled(token_data.get("cannot_sell_all")):
        score -= 45
        risks.append("cannot_sell_all")
        hard_veto = True

    if _flag_enabled(token_data.get("is_honeypot")):
        score -= 60
        risks.append("honeypot_flag")
        hard_veto = True

    if _flag_enabled(token_data.get("hidden_owner")):
        score -= 14
        risks.append("hidden_owner")

    if _flag_enabled(token_data.get("owner_change_balance")):
        score -= 15
        risks.append("owner_can_change_balance")

    if _flag_enabled(token_data.get("selfdestruct")):
        score -= 12
        risks.append("selfdestruct_enabled")

    if _flag_enabled(token_data.get("trading_cooldown")):
        score -= 8
        risks.append("trading_cooldown")

    buy_tax = _as_float(token_data.get("buy_tax"), 0.0) or 0.0
    sell_tax = _as_float(token_data.get("sell_tax"), 0.0) or 0.0
    if max(buy_tax, sell_tax) >= 20:
        score -= 18
        risks.append("high_tax")
    if abs(buy_tax - sell_tax) >= 8:
        score -= 8
        risks.append("tax_conflict")

    return SafetyProviderResult(
        provider="goplus",
        available=True,
        score=_round_score(score),
        hard_veto=hard_veto,
        risks=sorted(set(risks)),
        details={
            "buy_tax": buy_tax,
            "sell_tax": sell_tax,
        },
    )


async def _run_honeypot(client: httpx.AsyncClient, intake: IntakeResult) -> SafetyProviderResult:
    chain_id = get_honeypot_chain_id(intake.chain)
    if not chain_id:
        return SafetyProviderResult(provider="honeypot", available=False, risks=["honeypot_unsupported_chain"])

    headers = {"X-API-KEY": CONFIG["honeypot_api_key"]} if CONFIG["honeypot_api_key"] else None
    ### LIVE API ###
    payload = await _fetch_json(
        client,
        f"{CONFIG['honeypot_base_url']}/v2/IsHoneypot",
        params={"address": intake.token_address, "chainID": chain_id, "pair": intake.pair_address},
        headers=headers,
    )

    score = 70.0
    risks: List[str] = []
    hard_veto = False
    honeypot_result = payload.get("honeypotResult") or {}
    simulation_result = payload.get("simulationResult") or {}
    summary = payload.get("summary") or {}

    if honeypot_result.get("isHoneypot") is True:
        score -= 70
        risks.append("honeypot_detected")
        hard_veto = True

    buy_tax = _as_float(simulation_result.get("buyTax"), 0.0) or 0.0
    sell_tax = _as_float(simulation_result.get("sellTax"), 0.0) or 0.0
    if max(buy_tax, sell_tax) >= 20:
        score -= 18
        risks.append("high_tax")
    if abs(buy_tax - sell_tax) >= 8:
        score -= 10
        risks.append("tax_conflict")

    risk = (summary.get("risk") or "").strip().lower()
    if risk in {"high", "very_high"}:
        score -= 22
        risks.append(f"honeypot_summary_{risk}")
    elif risk == "medium":
        score -= 10

    return SafetyProviderResult(
        provider="honeypot",
        available=True,
        score=_round_score(score),
        hard_veto=hard_veto,
        risks=sorted(set(risks)),
        details={
            "buy_tax": buy_tax,
            "sell_tax": sell_tax,
            "summary_risk": risk or None,
        },
    )


async def _run_tokensniffer(client: httpx.AsyncClient, intake: IntakeResult) -> SafetyProviderResult:
    api_key = CONFIG["tokensniffer_api_key"]
    chain_id = get_tokensniffer_chain_id(intake.chain)
    if not api_key or not chain_id:
        missing_reason = "tokensniffer_api_key_missing" if not api_key else "tokensniffer_unsupported_chain"
        return SafetyProviderResult(provider="tokensniffer", available=False, risks=[missing_reason])

    ### LIVE API ###
    payload = await _fetch_json(
        client,
        f"{CONFIG['tokensniffer_base_url']}/api/v2/tokens/{chain_id}/{intake.token_address}",
        params={"apikey": api_key, "include_metrics": "true", "include_tests": "true"},
    )

    score = _as_float(payload.get("score"), 0.0) or 0.0
    risks: List[str] = []
    hard_veto = False

    if payload.get("is_flagged") is True:
        risks.append("tokensniffer_flagged")
        score = min(score, 25.0)
        hard_veto = True

    if payload.get("swap_simulation", {}).get("sell_fee") not in {None, ""}:
        sell_fee = _as_float(payload["swap_simulation"].get("sell_fee"), 0.0) or 0.0
        if sell_fee >= 20:
            risks.append("high_tax")
            score = min(score, 45.0)

    return SafetyProviderResult(
        provider="tokensniffer",
        available=True,
        score=_round_score(score),
        hard_veto=hard_veto,
        risks=sorted(set(risks)),
        details={"status": payload.get("status"), "message": payload.get("message")},
    )


def _fallback_safety(intake: IntakeResult) -> SafetyProviderResult:
    ### HEURISTIC ###
    score = 60.0
    risks: List[str] = []
    if intake.liquidity_usd < 5000:
        score -= 18
        risks.append("thin_liquidity")
    if intake.paid_order_count > 0 and intake.boost_count > 0:
        score -= 4
        risks.append("boosted_marketing_push")
    if intake.price_change_24h_pct > 90:
        score -= 8
        risks.append("parabolic_move")
    return SafetyProviderResult(
        provider="fallback",
        available=True,
        score=_round_score(score),
        hard_veto=False,
        risks=sorted(set(risks + ["safety_heuristic_only"])),
        details={},
    )


async def run_stage(client: httpx.AsyncClient, intake: IntakeResult) -> SafetyResult:
    providers: Dict[str, SafetyProviderResult] = {}
    for provider_name, provider_runner in (
        ("goplus", _run_goplus),
        ("honeypot", _run_honeypot),
        ("tokensniffer", _run_tokensniffer),
    ):
        try:
            providers[provider_name] = await provider_runner(client, intake)
        except Exception as exc:
            providers[provider_name] = SafetyProviderResult(
                provider=provider_name,
                available=False,
                risks=[f"{provider_name}_error"],
                details={"error": str(exc)},
            )

    live_scores = [provider.score for provider in providers.values() if provider.available and provider.score is not None]
    if not live_scores:
        providers["fallback"] = _fallback_safety(intake)
        live_scores = [providers["fallback"].score]

    risks = []
    hard_veto = False
    tax_conflict = False
    for provider in providers.values():
        risks.extend(provider.risks)
        hard_veto = hard_veto or provider.hard_veto
        tax_conflict = tax_conflict or ("tax_conflict" in provider.risks)

    safety_score = _round_score(mean(live_scores))
    if hard_veto or safety_score < 30:
        risk_level = "critical"
    elif safety_score < 50:
        risk_level = "high"
    elif safety_score < 70:
        risk_level = "medium"
    else:
        risk_level = "low"

    return SafetyResult(
        safety_score=safety_score,
        risk_level=risk_level,
        hard_veto=hard_veto,
        tax_conflict=tax_conflict,
        risks=sorted(set(risks)),
        providers=providers,
    )
