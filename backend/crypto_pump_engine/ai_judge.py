from __future__ import annotations

import os
import json
import asyncio
from typing import Any, Dict, List, Optional

from openai import OpenAI


ALLOWED_ACTIONS = {"BUY_NOW", "WATCH", "AVOID", "SELL", "NO_ACTION"}
ALLOWED_VERDICTS = {
    "Strong Pump",
    "Pump Watch",
    "Noise",
    "Distribution Risk",
    "Strong Dump",
    "Rug Risk",
}
ALLOWED_SIGNAL_TRUTH = {"real", "likely_real", "mixed", "likely_noise", "noise"}
ALLOWED_TIMING = {"early", "developing", "late", "exhausted"}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _safe_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    return []


def _contains_any(risks: List[str], needles: List[str]) -> bool:
    hay = {str(x).strip().lower() for x in risks}
    return any(n.lower() in hay for n in needles)


def _detect_hard_veto(diagnostics: Dict[str, Any], result_payload: Dict[str, Any]) -> bool:
    safety = diagnostics.get("safety") or {}
    return bool(safety.get("hard_veto"))


def build_llm_evidence_payload(
    chain: str,
    pair_address: str,
    token_address: Optional[str],
    result_payload: Dict[str, Any],
    diagnostics: Dict[str, Any],
) -> Dict[str, Any]:
    features = result_payload.get("features") or {}
    result_risks = _safe_list(result_payload.get("risks"))
    safety = diagnostics.get("safety") or {}
    holders = diagnostics.get("holders") or {}
    social = diagnostics.get("social") or {}

    hard_veto = _detect_hard_veto(diagnostics, result_payload)

    return {
        "asset": {
            "chain": chain,
            "pair_address": pair_address,
            "token_address": token_address,
            "candidate_id": result_payload.get("candidate_id", ""),
        },
        "market": {
            "liquidity_usd": _safe_float(features.get("liquidity_usd")),
            "volume_24h_usd": _safe_float(features.get("volume_24h_usd")),
            "volume_liquidity_ratio": _safe_float(features.get("volume_liquidity_ratio")),
            "buy_sell_ratio": _safe_float(features.get("buy_sell_ratio")),
            "pair_age_minutes": _safe_float(features.get("pair_age_minutes")),
        },
        "safety": {
            "score": _safe_float(safety.get("safety_score")),
            "risk_level": str(safety.get("risk_level") or "unknown"),
            "hard_veto": hard_veto,
            "risks": _safe_list(safety.get("risks")),
        },
        "holders": {
            "concentration": _safe_float(holders.get("concentration")),
            "top_1_share": _safe_float(holders.get("top_1_share")),
            "top_5_share": _safe_float(holders.get("top_5_share")),
            "top_10_share": _safe_float(holders.get("top_10_share")),
            "risks": _safe_list(holders.get("risks")),
        },
        "social": {
            "score": _safe_float(social.get("social_score")),
            "boost_count": int(_safe_float(social.get("boost_count"))),
            "source_mode": str(social.get("source_mode") or ""),
            "risks": _safe_list(social.get("risks")),
        },
        "rule_engine": {
            "action": str(result_payload.get("action") or ""),
            "verdict": str(result_payload.get("verdict") or ""),
            "score": _safe_float(result_payload.get("score")),
            "risks": result_risks,
        },
    }


def should_run_ai_judge(evidence: Dict[str, Any]) -> bool:
    rule_engine = evidence.get("rule_engine") or {}
    market = evidence.get("market") or {}
    safety = evidence.get("safety") or {}

    score = _safe_float(rule_engine.get("score"))
    verdict = str(rule_engine.get("verdict") or "")
    action = str(rule_engine.get("action") or "")
    risks = _safe_list(rule_engine.get("risks"))
    buy_sell_ratio = _safe_float(market.get("buy_sell_ratio"))
    hard_veto = bool(safety.get("hard_veto"))

    if hard_veto:
        return True
    if score >= 55:
        return True
    if verdict in {"Pump Watch", "Distribution Risk", "Strong Pump", "Strong Dump"}:
        return True
    if action in {"WATCH", "BUY_NOW", "SELL", "AVOID"}:
        return True
    if risks:
        return True
    if buy_sell_ratio != 0:
        return True

    return False


def _fallback_ai_judge(evidence: Dict[str, Any]) -> Dict[str, Any]:
    safety = evidence.get("safety") or {}
    market = evidence.get("market") or {}
    holders = evidence.get("holders") or {}
    rule_engine = evidence.get("rule_engine") or {}

    hard_veto = bool(safety.get("hard_veto"))
    concentration = _safe_float(holders.get("concentration"))
    buy_sell_ratio = _safe_float(market.get("buy_sell_ratio"))
    score = _safe_float(rule_engine.get("score"))
    risks = _safe_list(rule_engine.get("risks"))

    final_action = str(rule_engine.get("action") or "WATCH")
    final_verdict = str(rule_engine.get("verdict") or "Pump Watch")
    rule_action = str(rule_engine.get("action") or "")
    rule_verdict = str(rule_engine.get("verdict") or "")

    probable_scenario = "weak_signal"
    if rule_verdict == "Strong Pump":
        probable_scenario = "speculative_pump"
    elif rule_verdict == "Distribution Risk":
        probable_scenario = "distribution"
    elif rule_verdict == "Rug Risk":
        probable_scenario = "rug_risk"

    signal_truth = "mixed"
    timing = "developing"
    why_now: List[str] = []
    red_flags: List[str] = []
    override_preliminary = False
    override_reason = ""

    if hard_veto:
        final_action = "AVOID"
        final_verdict = "Rug Risk"
        probable_scenario = "rug_risk"
        signal_truth = "likely_noise"
        timing = "late"
        why_now = ["Hard veto conditions present."]
        red_flags = ["hard_veto"]
        override_preliminary = (
            str(rule_engine.get("action") or "") != "AVOID"
            or str(rule_engine.get("verdict") or "") != "Rug Risk"
        )
        override_reason = "Hard risk guard overrides preliminary result." if override_preliminary else ""
    elif concentration >= 0.70:
        final_action = "AVOID"
        final_verdict = "Distribution Risk"
        probable_scenario = "distribution"
        signal_truth = "likely_noise"
        timing = "late"
        why_now = ["Holder concentration dominates the setup."]
        red_flags = ["extreme_concentration"]
        override_preliminary = str(rule_engine.get("action") or "") != "AVOID"
        override_reason = "Concentration risk dominates bullish setup." if override_preliminary else ""
    elif "seller_pressure" in risks and score >= 75:
        final_action = "WATCH"
        final_verdict = "Pump Watch"
        probable_scenario = "speculative_pump"
        signal_truth = "mixed"
        timing = "developing"
        why_now = ["Momentum exists but seller pressure prevents stronger conviction."]
        red_flags = ["seller_pressure"]
        override_preliminary = str(rule_engine.get("action") or "") == "BUY_NOW"
        override_reason = "Seller pressure downgrades the setup from immediate buy to watch." if override_preliminary else ""
    elif score >= 80 and buy_sell_ratio >= 1.15:
        final_action = "BUY_NOW"
        final_verdict = "Strong Pump"
        probable_scenario = "real_accumulation"
        signal_truth = "likely_real"
        timing = "developing"
        why_now = ["Momentum, flow, and structure are aligned."]
        red_flags = []
        override_preliminary = str(rule_engine.get("action") or "") != "BUY_NOW"
        override_reason = "Evidence is stronger than the preliminary result." if override_preliminary else ""
    elif str(rule_engine.get("action") or "") == "BUY_NOW" and str(rule_engine.get("verdict") or "") == "Strong Pump":
        final_action = "BUY_NOW"
        final_verdict = "Strong Pump"
        probable_scenario = "speculative_pump"
        signal_truth = "mixed"
        timing = "developing"
        why_now = ["Rule engine still indicates a strong pump setup, but fallback AI has no stronger downgrade signal."]
        red_flags = _safe_list(rule_engine.get("risks"))
        override_preliminary = False
        override_reason = ""
    elif str(rule_engine.get("verdict") or "") == "Noise" or score < 60:
        final_action = "WATCH" if str(rule_engine.get("action") or "") == "WATCH" else "NO_ACTION"
        final_verdict = "Noise"
        probable_scenario = "weak_signal"
        signal_truth = "noise"
        timing = "late" if score < 50 else "developing"
        why_now = ["Evidence is too thin or contradictory."]
        red_flags = ["low_conviction"]
        override_preliminary = False
        override_reason = ""

    return {
        "final_action": final_action,
        "final_verdict": final_verdict,
        "confidence": int(min(95, max(35, round(score)))),
        "probable_scenario": probable_scenario,
        "signal_truth": signal_truth,
        "timing": timing,
        "why_now": why_now,
        "red_flags": red_flags,
        "override_preliminary": override_preliminary,
        "override_reason": override_reason,
        "ai_reasoning_summary": "Fallback AI judge result.",
        "source": "fallback",
    }



def run_llm_decision_layer(evidence: Dict[str, Any]) -> Dict[str, Any]:
    openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()
    openrouter_api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    openrouter_base_url = os.getenv("OPENROUTER_BASE_URL", "").strip() or "https://openrouter.ai/api/v1"
    openai_model = os.getenv("PUMP_ENGINE_AI_MODEL", "").strip() or "gpt-4.1-mini"

    prompt = f"""Evaluate this crypto setup and return strict JSON.

Evidence:
{json.dumps(evidence, ensure_ascii=False)}

Return EXACTLY this JSON schema:
{{
  "final_action": "BUY_NOW | WATCH | AVOID | SELL | NO_ACTION",
  "final_verdict": "Strong Pump | Pump Watch | Noise | Distribution Risk | Strong Dump | Rug Risk",
  "confidence": 0,
  "probable_scenario": "real_accumulation | speculative_pump | coordinated_noise | distribution | rug_risk | weak_signal",
  "signal_truth": "real | likely_real | mixed | likely_noise | noise",
  "timing": "early | developing | late | exhausted",
  "why_now": [],
  "red_flags": [],
  "override_preliminary": false,
  "override_reason": "",
  "ai_reasoning_summary": ""
}}

Rules:
- Never return BUY_NOW if safety.hard_veto is true.
- Prefer AVOID when concentration, weak liquidity, or rug-risk dominates.
- Prefer WATCH when signal is interesting but incomplete.
- Prefer NO_ACTION or Noise when evidence is thin or contradictory.
- If your final_action is different from rule_engine.action, you MUST set override_preliminary=true.
- If rule_engine.action is BUY_NOW and you downgrade to WATCH or AVOID, override_preliminary must be true and override_reason must explain the downgrade.
- If rule_engine.action is AVOID or WATCH and you upgrade the setup, override_preliminary must be true and override_reason must explain the upgrade.
- Keep explanations concise and factual.
Respond ONLY with raw JSON, no markdown fences.
"""

    if openai_api_key or openrouter_api_key:
        try:
            if openrouter_api_key:
                client = OpenAI(
                    api_key=openrouter_api_key,
                    base_url=openrouter_base_url,
                )
                model = openai_model
                source_prefix = "openrouter"
            else:
                client = OpenAI(api_key=openai_api_key)
                model = openai_model
                source_prefix = "openai"

            response = client.chat.completions.create(
                model=model,
                temperature=0,
                max_tokens=700,
                response_format={"type": "json_object"},
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are the final decision layer for a crypto signal intelligence engine. "
                            "You classify whether the setup is real accumulation, speculative pump, coordinated noise, "
                            "distribution, rug-risk, or weak/non-actionable signal. "
                            "Use only the supplied evidence. Do not invent facts. "
                            "Respond only with valid JSON."
                        ),
                    },
                    {
                        "role": "user",
                        "content": prompt,
                    },
                ],
            )

            content = response.choices[0].message.content or "{}"
            parsed = json.loads(content)
            parsed["source"] = f"{source_prefix}:{model}"
            return _sanitize_ai_result(parsed)

        except Exception as exc:
            openai_fallback = _fallback_ai_judge(evidence)
            openai_fallback["ai_reasoning_summary"] = (
                f"Fallback AI judge result. OpenAI/OpenRouter error: {type(exc).__name__}: {exc}"
            )
            openai_fallback["red_flags"] = list(openai_fallback.get("red_flags") or []) + [
                f"openai_error:{type(exc).__name__}"
            ]
            openai_fallback["source"] = "fallback_after_openai_error"
            return openai_fallback

    return _fallback_ai_judge(evidence)

def _sanitize_ai_result(ai_result: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(ai_result or {})

    if result.get("final_action") not in ALLOWED_ACTIONS:
        result["final_action"] = "NO_ACTION"
    if result.get("final_verdict") not in ALLOWED_VERDICTS:
        result["final_verdict"] = "Noise"
    if result.get("signal_truth") not in ALLOWED_SIGNAL_TRUTH:
        result["signal_truth"] = "mixed"
    if result.get("timing") not in ALLOWED_TIMING:
        result["timing"] = "developing"

    result["confidence"] = int(min(100, max(0, _safe_float(result.get("confidence")))))
    result["why_now"] = _safe_list(result.get("why_now"))
    result["red_flags"] = _safe_list(result.get("red_flags"))
    result["override_preliminary"] = bool(result.get("override_preliminary"))
    result["override_reason"] = str(result.get("override_reason") or "")
    result["ai_reasoning_summary"] = str(result.get("ai_reasoning_summary") or "")
    result["source"] = str(result.get("source") or "unknown")

    return result


def merge_rule_and_ai(
    result_payload: Dict[str, Any],
    ai_result: Optional[Dict[str, Any]],
    evidence: Dict[str, Any],
) -> Dict[str, Any]:
    rule_action = str(result_payload.get("action") or "")
    rule_verdict = str(result_payload.get("verdict") or "")
    rule_score = _safe_float(result_payload.get("score"))
    hard_veto = bool((evidence.get("safety") or {}).get("hard_veto"))

    if not ai_result:
        return {
            "rule_engine": result_payload,
            "ai_judge": None,
            "final": {
                "action": rule_action,
                "verdict": rule_verdict,
                "confidence": int(round(rule_score)),
                "signal_truth": "mixed",
                "timing": "developing",
            },
        }

    ai = _sanitize_ai_result(ai_result)

    if hard_veto:
        forced_verdict = "Rug Risk"
        if ai.get("final_verdict") == "Distribution Risk":
            forced_verdict = "Distribution Risk"
        return {
            "rule_engine": result_payload,
            "ai_judge": ai,
            "final": {
                "action": "AVOID",
                "verdict": forced_verdict,
                "confidence": max(ai["confidence"], int(round(rule_score))),
                "signal_truth": ai["signal_truth"],
                "timing": ai["timing"],
            },
        }

    if not ai.get("override_preliminary"):
        return {
            "rule_engine": result_payload,
            "ai_judge": ai,
            "final": {
                "action": rule_action,
                "verdict": rule_verdict,
                "confidence": ai["confidence"],
                "signal_truth": ai["signal_truth"],
                "timing": ai["timing"],
            },
        }

    return {
        "rule_engine": result_payload,
        "ai_judge": ai,
        "final": {
            "action": ai["final_action"],
            "verdict": ai["final_verdict"],
            "confidence": ai["confidence"],
            "signal_truth": ai["signal_truth"],
            "timing": ai["timing"],
        },
    }
