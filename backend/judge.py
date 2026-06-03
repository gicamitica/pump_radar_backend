"""
PumpRadar Judge - Pas 5
Claude Haiku AI Judge - fara filtre stricte inainte.
Haiku decide tot: pump / dump / risk / watch / dex / early / avoid
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()
HAIKU_MODEL = "claude-haiku-4-5-20251001"
QWEN_URL = "http://127.0.0.1:8088/v1/chat/completions"

ALLOWED_CATEGORIES = {"pump", "dump", "risk", "watch", "avoid", "dex", "early"}

SYSTEM_PROMPT = """You are PumpRadar AI Judge — a crypto signal classifier.

Your job: classify each token setup into exactly one category.

Categories:
- pump: clear bullish momentum with volume confirmation
- dump: selling pressure, price breakdown with volume
- risk: distribution risk, late pump reversal, or high holder concentration
- watch: interesting setup but missing confirmation — monitor closely
- early: Telegram/social signal present but market hasn't moved yet
- dex: DEX-only signal, no social confirmation, treat as speculative
- avoid: honeypot, rug risk, scam database hit, or no useful data

You receive per token:
- price_change_h1, price_change_h24: recent price moves
- volume_h24, volume_h1: trading volume
- reserve_usd: pool liquidity
- buy_sell_ratio_h1: >1 = buyers dominating, <1 = sellers dominating
- volume_liquidity_ratio: high ratio = unusual activity
- sources: where signal was detected (telegram, reddit, cointelegraph, dexscreener, geckoterminal)
- mentions: how many sources mentioned this token
- red_flags: security issues (honeypot, blacklist, defi_rekt_database, whale_dump_risk, high_tax)
- whale_accumulation: true if large wallets are buying
- whale_dump_risk: true if large wallets are selling to exchanges

Decision logic:
1. SAFETY FIRST: red_flags with honeypot or cannot_sell_all = always avoid
2. WHALE SIGNALS: whale_accumulation=true + price_change_h24 > 0 = strong pump signal
3. WHALE DUMP: whale_dump_risk=true = risk or dump category
4. REKT DATABASE: defi_rekt_database in red_flags = downgrade one level (pump→watch, watch→avoid)
5. NOISE FILTER: single source (mentions=1) + no whale data + no telegram = watch or dex only
6. MULTI-SOURCE BOOST: mentions >= 3 across different sources = upgrade confidence
7. VOLUME CONFIRMATION: volume_liquidity_ratio > 0.5 = market is active, increases confidence
8. EARLY DETECTION: telegram source + price_change_h24 < 3 = early category
9. DEX ONLY: only dexscreener/geckoterminal sources + mentions=1 = dex category
10. PRE-PUMP ACTIVITY (Sapienza): volume_h1 > (volume_h24/24)*3 AND price_change_h24 < 2 = insider movement before announcement, set pre_pump_activity=true, upgrade early confidence +15
11. MANIPULATION SIGNAL (Bayi-Hu): telegram in sources AND mentions >= 3 = coordinated pump group activity, set manipulation_probability > 70, treat as high-confidence early or pump
12. VOLUME ANOMALY (binancePump): buy_sell_ratio_h1 > 2.0 AND volume_h1 > 0 = confirmed real buying anomaly, upgrade confidence +10

Be balanced — not overly conservative. A token with good momentum and multi-source confirmation SHOULD be pump.
A token with only Reddit/CT mention and no volume = watch.
Never return avoid unless there are actual security red flags or zero useful data."""


def _build_candidate_summary(candidate: Dict) -> Dict:
    market = candidate.get("market") or {}
    security = candidate.get("security") or {}
    whale = candidate.get("whale") or {}
    defi_rekt = candidate.get("defi_rekt") or {}
    pc = market.get("price_change_pct") or {}
    vol = market.get("volume_usd") or {}
    tx = market.get("transactions") or {}

    return {
        "symbol": candidate.get("symbol"),
        "sources": candidate.get("sources", []),
        "mentions": candidate.get("mentions", 1),
        "network": candidate.get("network"),
        "price_usd": candidate.get("price_usd"),
        "reserve_usd": candidate.get("reserve_usd"),
        "volume_h24": vol.get("h24"),
        "volume_h1": vol.get("h1"),
        "price_change_h1": pc.get("h1"),
        "price_change_h24": pc.get("h24"),
        "buy_sell_ratio_h1": tx.get("h1_buy_sell_ratio"),
        "volume_liquidity_ratio": market.get("volume_liquidity_ratio_h24"),
        "red_flags": candidate.get("red_flags") or [],
        "whale_accumulation": whale.get("accumulation_detected", False),
        "whale_dump_risk": whale.get("dump_risk", False),
        "whale_score": whale.get("whale_score", 0),
        "is_rekt": defi_rekt.get("is_rekt", False),
        "buy_tax": security.get("buy_tax"),
        "sell_tax": security.get("sell_tax"),
        "is_open_source": security.get("is_open_source"),
    }


async def call_haiku(candidates_data: List[Dict]) -> Optional[Dict]:
    if not ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY lipsa")
        return None

    prompt = f"""Classify these crypto token candidates. Be balanced and data-driven.

Candidates:
{json.dumps(candidates_data, indent=2, default=str)[:12000]}

Return ONLY this JSON:
{{
  "classifications": [
    {{
      "symbol": "TOKEN",
      "category": "pump|dump|risk|watch|early|dex|avoid",
      "confidence": 0-100,
      "reason": "max 15 words explaining the decision",
      "verdict": "Short Pump|Dump Risk|Distribution Risk|Watch Setup|Early Signal|DEX Watch|Avoid",
      "pre_pump_activity": true or false,
      "manipulation_probability": 0-100,
      "dump_risk_level": "low|medium|high"
    }}
  ],
  "market_summary": "2-3 sentences about overall market context"
}}

Respond ONLY with raw JSON, no markdown."""

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": HAIKU_MODEL,
                    "max_tokens": 2000,
                    "temperature": 0.1,
                    "system": SYSTEM_PROMPT,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
        if resp.status_code != 200:
            logger.warning(f"Haiku error {resp.status_code}: {resp.text[:200]}")
            return None
        content = resp.json().get("content") or []
        text = (content[0].get("text") or "").strip() if content else ""
        if text.startswith("```"):
            text = "\n".join(l for l in text.splitlines() if not l.strip().startswith("```")).strip()
        return json.loads(text)
    except Exception as e:
        logger.warning(f"Haiku call error: {e}")
        return None


async def call_qwen_fallback(candidates_data: List[Dict]) -> Optional[Dict]:
    try:
        prompt = (
            f"Classify these crypto tokens as pump/dump/risk/watch/early/dex/avoid. "
            f"Return JSON with classifications array. Tokens: "
            f"{json.dumps([c['symbol'] for c in candidates_data])}"
        )
        async with httpx.AsyncClient(timeout=35) as client:
            resp = await client.post(
                QWEN_URL,
                json={
                    "model": "local-qwen",
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.0,
                    "max_tokens": 500,
                },
            )
        if resp.status_code != 200:
            return None
        text = ((resp.json().get("choices") or [{}])[0].get("message") or {}).get("content", "")
        return json.loads(text)
    except Exception as e:
        logger.debug(f"Qwen fallback error: {e}")
        return None


def _deterministic_fallback(candidates: List[Dict]) -> Dict:
    classifications = []
    for c in candidates:
        red_flags = c.get("red_flags") or []
        market = c.get("market") or {}
        whale = c.get("whale") or {}
        pc = market.get("price_change_pct") or {}
        tx = market.get("transactions") or {}
        reserve = c.get("reserve_usd", 0) or 0
        bs_ratio = tx.get("h1_buy_sell_ratio", 1) or 1
        ch1 = pc.get("h1", 0) or 0
        ch24 = pc.get("h24", 0) or 0
        sources = set(c.get("sources") or [])
        mentions = c.get("mentions", 1)

        if "honeypot" in red_flags or "cannot_sell_all" in red_flags:
            cat, verdict, reason = "avoid", "Avoid", "Honeypot or sell restriction"
        elif whale.get("accumulation_detected") and ch24 > 0:
            cat, verdict, reason = "pump", "Short Pump", "Whale accumulation with positive momentum"
        elif whale.get("dump_risk"):
            cat, verdict, reason = "risk", "Distribution Risk", "Whale dump risk detected"
        elif "defi_rekt_database" in red_flags:
            cat, verdict, reason = "watch", "Watch Setup", "Token in REKT database"
        elif reserve < 10000:
            cat, verdict, reason = "watch", "Watch Setup", "Low liquidity"
        elif ch24 > 200 and ch1 < -5:
            cat, verdict, reason = "risk", "Distribution Risk", "Late pump reversal"
        elif bs_ratio > 1.5 and ch1 > 3 and reserve > 50000:
            cat, verdict, reason = "pump", "Short Pump", "Strong buy pressure"
        elif ch1 < -5 and ch24 < -10:
            cat, verdict, reason = "dump", "Dump Risk", "Selling pressure"
        elif "telegram" in sources and mentions >= 2 and ch24 < 3:
            cat, verdict, reason = "early", "Early Signal", "Telegram signal, market not moved"
        elif sources <= {"dexscreener", "geckoterminal"} and mentions == 1:
            cat, verdict, reason = "dex", "DEX Watch", "DEX only signal"
        elif ch24 > 10 and mentions >= 2:
            cat, verdict, reason = "pump", "Pump Watch", "Multi-source positive momentum"
        else:
            cat, verdict, reason = "watch", "Watch Setup", "Insufficient confirmation"

        classifications.append({
            "symbol": c.get("symbol"),
            "category": cat,
            "confidence": 45,
            "reason": reason,
            "verdict": verdict,
            "source": "deterministic_fallback",
        })

    return {
        "classifications": classifications,
        "market_summary": "Deterministic fallback active.",
        "source": "fallback",
    }


async def judge_candidates(candidates: List[Dict]) -> List[Dict]:
    if not candidates:
        return []

    summaries = [_build_candidate_summary(c) for c in candidates]
    batch_size = 15
    all_classifications = {}

    for i in range(0, len(summaries), batch_size):
        batch = summaries[i:i + batch_size]
        batch_candidates = candidates[i:i + batch_size]

        result = await call_haiku(batch)
        if not result:
            result = await call_qwen_fallback(batch)
        if not result:
            result = _deterministic_fallback(batch_candidates)

        for clf in (result.get("classifications") or []):
            sym = clf.get("symbol")
            if sym:
                all_classifications[sym] = clf

        if i + batch_size < len(summaries):
            await asyncio.sleep(0.5)

    signals = []
    for candidate in candidates:
        sym = candidate.get("symbol")
        clf = all_classifications.get(sym)
        if not clf:
            continue

        category = (clf.get("category") or "watch").lower()
        if category not in ALLOWED_CATEGORIES:
            category = "watch"

        if category == "avoid":
            continue

        market = candidate.get("market") or {}
        pc = market.get("price_change_pct") or {}
        vol = market.get("volume_usd") or {}
        whale = candidate.get("whale") or {}

        signals.append({
            "symbol": sym,
            "name": sym,
            "category": category,
            "signal_type": category,
            "verdict": clf.get("verdict", "Watch Setup"),
            "confidence": int(clf.get("confidence") or 50),
            "reason": clf.get("reason", ""),
            "ai_source": clf.get("source", "claude_haiku"),
            "sources": candidate.get("sources", []),
            "mentions": candidate.get("mentions", 1),
            "network": candidate.get("network"),
            "token_address": candidate.get("token_address"),
            "pool_address": candidate.get("pool_address"),
            "pool_url": candidate.get("pool_url"),
            "price_usd": candidate.get("price_usd"),
            "reserve_usd": candidate.get("reserve_usd"),
            "volume_h24": vol.get("h24"),
            "price_change_h1": pc.get("h1"),
            "price_change_h24": pc.get("h24"),
            "buy_sell_ratio_h1": (market.get("transactions") or {}).get("h1_buy_sell_ratio"),
            "red_flags": candidate.get("red_flags") or [],
            "whale_accumulation": whale.get("accumulation_detected", False),
            "whale_score": whale.get("whale_score", 0),
            "multi_source": len(candidate.get("sources", [])) > 1,
            "pre_pump_activity": clf.get("pre_pump_activity", False),
            "manipulation_probability": int(clf.get("manipulation_probability") or 0),
            "dump_risk_level": clf.get("dump_risk_level", "low"),
        })

    logger.info(f"Judge: {len(signals)} semnale din {len(candidates)} candidati")
    return signals
