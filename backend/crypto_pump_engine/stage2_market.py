from __future__ import annotations

from datetime import datetime, timezone

from .models import IntakeResult, MarketResult


def _clamp(value: float, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return max(minimum, min(maximum, value))


def run_stage(intake: IntakeResult) -> MarketResult:
    ### HEURISTIC ###
    liquidity = float(intake.liquidity_usd or 0.0)
    volume = float(intake.volume_24h_usd or 0.0)
    buys = float(intake.buys_24h or 0)
    sells = float(intake.sells_24h or 0)
    price_change = float(intake.price_change_24h_pct or 0.0)

    volume_liquidity_ratio = volume / liquidity if liquidity > 0 else 0.0
    buy_sell_ratio = (buys / sells) if sells > 0 else (buys if buys > 0 else 0.0)

    pair_age_minutes = None
    if intake.pair_created_at_ms:
        created_at = datetime.fromtimestamp(intake.pair_created_at_ms / 1000, tz=timezone.utc)
        pair_age_minutes = (datetime.now(timezone.utc) - created_at).total_seconds() / 60

    momentum_score = _clamp(
        (price_change * 1.2)
        + (min(volume_liquidity_ratio, 8.0) * 9)
        + (min(buy_sell_ratio, 4.0) * 8)
    )

    freshness_bonus = 0.0
    risks = []
    notes = []

    if pair_age_minutes is not None:
        if pair_age_minutes <= 180:
            freshness_bonus = 12.0
            notes.append("very_young_pair")
        elif pair_age_minutes <= 1440:
            freshness_bonus = 6.0
            notes.append("young_pair")
        elif pair_age_minutes >= 10080:
            risks.append("stale_pair")

    if liquidity < 5000:
        risks.append("thin_liquidity")
    if volume_liquidity_ratio < 0.3:
        risks.append("weak_turnover")
    if buy_sell_ratio < 0.85:
        risks.append("seller_pressure")

    market_score = _clamp(
        (liquidity / 500.0)
        + momentum_score * 0.55
        + min(volume_liquidity_ratio, 6.0) * 5
        + freshness_bonus
    )

    if price_change <= -18 and buy_sell_ratio < 0.85:
        market_verdict = "dump_breakdown"
    elif market_score >= 75 and volume_liquidity_ratio >= 1.0 and buy_sell_ratio >= 1.15:
        market_verdict = "pump_breakout"
    elif market_score >= 58:
        market_verdict = "pump_watch"
    elif price_change <= -8:
        market_verdict = "distribution"
    else:
        market_verdict = "neutral"

    return MarketResult(
        liquidity_usd=round(liquidity, 2),
        volume_24h_usd=round(volume, 2),
        volume_liquidity_ratio=round(volume_liquidity_ratio, 4),
        buy_sell_ratio=round(buy_sell_ratio, 4),
        pair_age_minutes=round(pair_age_minutes, 2) if pair_age_minutes is not None else None,
        momentum_score=round(momentum_score, 2),
        market_score=round(market_score, 2),
        market_verdict=market_verdict,
        risks=sorted(set(risks)),
        notes=notes,
    )
