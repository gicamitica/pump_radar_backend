from __future__ import annotations

from .models import HoldersResult, MarketResult, RulesResult, SafetyResult, SocialResult


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, round(value, 2)))


def _suspect_gate(
    market: MarketResult,
    safety: SafetyResult,
    holders: HoldersResult,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []

    if safety.hard_veto:
        reasons.append("hard_veto")

    if safety.safety_score < 55:
        reasons.append("low_safety_score")

    if holders.concentration >= 0.45:
        reasons.append("holder_concentration_high")
    elif holders.concentration >= 0.30:
        reasons.append("holder_concentration_elevated")

    if market.liquidity_usd < 25000:
        reasons.append("thin_liquidity")

    if market.market_verdict in {"distribution", "dump_breakdown", "seller_pressure", "no_liquidity"}:
        reasons.append(f"market_{market.market_verdict}")

    return (len(reasons) > 0, reasons)


def run_stage(
    market: MarketResult,
    safety: SafetyResult,
    holders: HoldersResult,
    social: SocialResult,
) -> RulesResult:
    ### HEURISTIC ###
    holder_quality = max(0.0, 100.0 - (holders.concentration * 100.0))
    score = _clamp(
        (market.market_score * 0.46)
        + (safety.safety_score * 0.34)
        + (holder_quality * 0.15)
        + (social.social_score * 0.05)
    )

    risks = sorted(set(market.risks + safety.risks + holders.risks + social.risks))
    suspect, suspect_reasons = _suspect_gate(market, safety, holders)
    risks = sorted(set(risks + suspect_reasons))

    # SUSPECT FLOW
    if suspect:
        if safety.hard_veto:
            return RulesResult(
                action="AVOID",
                verdict="Rug Risk",
                score=min(score, 20.0),
                risks=sorted(set(risks + ["rug_risk"]))
            )

        if market.market_verdict == "dump_breakdown" and market.market_score >= 55:
            return RulesResult(
                action="SELL",
                verdict="Strong Dump",
                score=score,
                risks=sorted(set(risks + ["dump_breakdown"]))
            )

        if holders.concentration >= 0.45:
            if social.social_score >= 40 and market.market_score >= 58:
                return RulesResult(
                    action="WATCH",
                    verdict="High-Risk Pump",
                    score=min(score, 59.0),
                    risks=sorted(set(risks + ["high_risk_setup"]))
                )
            return RulesResult(
                action="AVOID",
                verdict="Distribution Risk",
                score=min(score, 45.0),
                risks=sorted(set(risks + ["distribution_risk"]))
            )

        if market.market_verdict == "distribution" and holders.concentration >= 0.30:
            return RulesResult(
                action="WATCH",
                verdict="Distribution Risk",
                score=min(score, 58.0),
                risks=sorted(set(risks + ["distribution_risk"]))
            )

        if market.market_score >= 58 and social.social_score >= 35:
            return RulesResult(
                action="WATCH",
                verdict="High-Risk Pump",
                score=min(score, 59.0),
                risks=sorted(set(risks + ["high_risk_setup"]))
            )

        if market.market_score < 35 and safety.safety_score < 40:
            return RulesResult(
                action="AVOID",
                verdict="Avoid",
                score=min(score, 35.0),
                risks=sorted(set(risks + ["weak_setup"]))
            )

        return RulesResult(
            action="WATCH",
            verdict="Noise",
            score=min(score, 54.0),
            risks=sorted(set(risks + ["insufficient_confirmation"]))
        )

    # CLEAN FLOW
    if market.market_verdict == "dump_breakdown" and market.market_score >= 55:
        return RulesResult(action="SELL", verdict="Strong Dump", score=score, risks=risks)

    if market.market_score >= 78 and safety.safety_score >= 70 and holders.concentration <= 0.25:
        return RulesResult(action="BUY_NOW", verdict="Strong Pump", score=score, risks=risks)

    if market.market_score >= 60 and safety.safety_score >= 52:
        return RulesResult(action="WATCH", verdict="Pump Watch", score=score, risks=risks)

    if market.market_score >= 58 and safety.safety_score < 52:
        return RulesResult(
            action="WATCH",
            verdict="High-Risk Pump",
            score=min(score, 59.0),
            risks=sorted(set(risks + ["high_risk_setup"]))
        )

    if market.market_score < 35 and safety.safety_score < 40:
        return RulesResult(
            action="AVOID",
            verdict="Avoid",
            score=min(score, 35.0),
            risks=sorted(set(risks + ["weak_setup"]))
        )

    return RulesResult(
        action="WATCH",
        verdict="Noise",
        score=min(score, 54.0),
        risks=sorted(set(risks + ["insufficient_confirmation"]))
    )
