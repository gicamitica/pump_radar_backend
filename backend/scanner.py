"""
PumpRadar Scanner - Pas 1+2
Colecteaza simboluri candidate din surse de semnale.
"""
from __future__ import annotations

import asyncio
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set

import httpx

logger = logging.getLogger(__name__)

MAJOR_TOKENS = {
    "BTC", "ETH", "USDT", "USDC", "BNB", "XRP", "ADA", "DOGE", "SOL",
    "DOT", "MATIC", "SHIB", "LTC", "AVAX", "UNI", "LINK", "ATOM", "TRX",
    "NEAR", "HBAR", "FIL", "ETC", "XMR", "ICP", "FTM", "SAND", "MANA",
    "AXS", "CAKE", "HYPE", "SUI", "APT", "OP", "ARB", "GMX", "DYDX",
    "LDO", "PENDLE", "WBTC", "WETH", "DAI", "BUSD", "STETH", "BITCOIN",
    "ETHEREUM", "SOLANA", "BINANCE", "RIPPLE", "CARDANO", "DOGECOIN",
}

STOPWORDS = {
    "USD", "NFT", "DAO", "DEX", "CEX", "ATH", "ATL", "ROI", "APY", "APR",
    "TVL", "AMA", "IDO", "ICO", "IEO", "USA", "API", "RSS", "AI", "THE",
    "FOR", "AND", "NOT", "ARE", "BUT", "YOU", "ALL", "CAN", "NEW", "NOW",
    "GET", "TOP", "HOW", "WHY", "PUMP", "DUMP", "MOON", "THIS", "WITH",
    "FROM", "THAT", "HAVE", "THEY", "WILL", "INTO", "MORE", "WHAT", "WHEN",
    "MAKE", "LIKE", "TIME", "ONLY", "THEM", "WELL", "MUCH", "VERY", "JUST",
    "OVER", "SOME", "ALSO", "THAN", "THEN", "BOTH", "BEEN", "HAVE", "EACH",
    "FIRST", "LAST", "LONG", "NEXT", "HIGH", "GOOD", "LIVE", "SHOW", "GIVE",
    "OPEN", "SEEM", "HELP", "TALK", "TURN", "MOVE", "PLAY", "COME", "DOES",
    "SAYS", "SAID", "AFTER", "BEFORE", "ABOUT", "WOULD", "COULD", "SHOULD",
    "THEIR", "THERE", "THESE", "THOSE", "WHICH", "WHILE", "EVERY", "NEVER",
    "ALWAYS", "OFTEN", "SINCE", "UNTIL", "BELOW", "ABOVE", "UNDER", "AGAIN",
    "STILL", "MIGHT", "SHALL", "GREAT", "SMALL", "LARGE", "EARLY", "LATER",
    "OTHER", "AFTER", "ALONG", "AMONG", "BEING", "DOING", "GOING", "KNOWN",
    "POINT", "PRICE", "STOCK", "BLOCK", "CHAIN", "TOKEN", "CRYPTO", "DEFI",
    "MARKET", "TRADE", "TRADING", "SIGNAL", "SIGNALS", "ALERT", "NEWS",
    "UPDATE", "REPORT", "LATEST", "RECENT", "TODAY", "WEEKLY", "DAILY",
    "BILLION", "MILLION", "PERCENT", "LAUNCH", "LISTING", "GOOGLE", "APPLE",
    "LAYER", "NETWORK", "PROTOCOL", "FINANCE", "CAPITAL", "GLOBAL", "WORLD",
    "OFFICIAL", "PLATFORM", "EXCHANGE", "WALLET", "SMART", "CONTRACT",
    "RUSSELL", "BITMINE", "UNISWAP", "SCAMMER", "SWISSBLOC", "MACHINE",
    "DAY", "ONE", "BUY", "SELL", "MAN", "WAY", "USE", "MAY", "SAY", "SEE",
    "TWO", "OUT", "OWN", "OLD", "ANY", "FEW", "FAR", "OFF", "LOT", "SET",
    "PUT", "RUN", "TRY", "ASK", "END", "WHY", "LET", "TEN", "SIX", "TEN",
    "LOW", "KEY", "HIT", "WIN", "BIG", "BAD", "HOT", "DID", "GOT", "HAD",
    "HAS", "WAS", "HIM", "HER", "HIS", "ITS", "OUR", "HIT", "FIT", "SIT",
    "BULL", "BEAR", "HOLD", "HODL", "FOMO", "RISK", "SAFE", "FAST", "SLOW",
    "HARD", "EASY", "FREE", "FULL", "HALF", "PART", "REAL", "FAKE", "TRUE",
    "BACK", "DOWN", "LEFT", "RISE", "FALL", "GROW", "DROP", "STOP", "KEEP",
    "TAKE", "WENT", "WENT", "WANT", "NEED", "FEEL", "TELL", "KNOW", "WORK",
    "YEAR", "WEEK", "HOUR", "MINE", "BANK", "FUND", "PLAN", "IDEA", "TEAM",
    "USER", "DATA", "CODE", "LIST", "FORM", "TYPE", "MODE", "RATE", "BASE",
    "CASE", "LINE", "SIDE", "LEAD", "MAIN", "NEXT", "PAST", "SOON", "BEST",
    "RACE", "GAME", "PLAY", "ROAD", "PATH", "STEP", "MOVE", "CALL", "DEAL",
}

BULLISH_KW = {
    "pump", "moon", "bullish", "buy", "long", "breakout", "ath", "gem",
    "launch", "early", "presale", "listing", "100x", "1000x", "undervalued",
    "hidden", "sleeping", "airdrop", "rally", "surge", "soar", "spike",
}

BEARISH_KW = {
    "dump", "crash", "bearish", "sell", "short", "rug", "scam", "exit",
    "collapse", "plunge", "drop", "fall", "decline",
}

TICKER_RE = re.compile(r"\\\$([A-Z]{2,10})")
WORD_RE = re.compile(r"\b([A-Z]{2,10})\b")

# Proiecte crypto cunoscute cu simbolul lor
CRYPTO_NAME_MAP = {
    "BITCOIN": "BTC", "ETHEREUM": "ETH", "SOLANA": "SOL", "CARDANO": "ADA",
    "DOGECOIN": "DOGE", "SHIBA": "SHIB", "AVALANCHE": "AVAX", "POLYGON": "MATIC",
    "CHAINLINK": "LINK", "UNISWAP": "UNI", "AAVE": "AAVE", "CURVE": "CRV",
    "ARBITRUM": "ARB", "OPTIMISM": "OP", "APTOS": "APT", "NEAR": "NEAR",
    "COSMOS": "ATOM", "POLKADOT": "DOT", "FILECOIN": "FIL", "RENDER": "RNDR",
    "INJECTIVE": "INJ", "CELESTIA": "TIA", "PYTH": "PYTH", "JUPITER": "JUP",
    "BONK": "BONK", "PEPE": "PEPE", "FLOKI": "FLOKI", "MATIC": "MATIC",
    "FANTOM": "FTM", "HEDERA": "HBAR", "INTERNET": "ICP", "MAKER": "MKR",
    "COMPOUND": "COMP", "SYNTHETIX": "SNX", "YEARN": "YFI", "SUSHI": "SUSHI",
    "RAYDIUM": "RAY", "ORCA": "ORCA", "DRIFT": "DRIFT", "MARINADE": "MNDE",
}

SUBREDDITS = [
    "CryptoMoonShots", "memecoins", "SatoshiStreetBets", "solana",
    "SolanaMemeCoins", "pumpfun", "CryptoCurrency", "altcoin", "defi",
]

CT_FEEDS = [
    "https://cointelegraph.com/rss",
    "https://cointelegraph.com/rss/tag/altcoin",
    "https://cointelegraph.com/rss/tag/defi",
]

DEXSCREENER_NETWORKS = ["solana", "ethereum", "bsc"]


def extract_symbols_from_text(text: str) -> List[str]:
    """Extrage simboluri din text - atat $TICKER cat si nume de proiecte."""
    results = []
    upper = text.upper()

    # 1. $TICKER explicit - cea mai sigura sursa
    dollar_tickers = TICKER_RE.findall(upper)
    for sym in dollar_tickers:
        if sym not in STOPWORDS and sym not in MAJOR_TOKENS and len(sym) >= 2:
            results.append(sym)

    # 2. Nume de proiecte cunoscute
    for name, symbol in CRYPTO_NAME_MAP.items():
        if name in upper and symbol not in MAJOR_TOKENS:
            results.append(symbol)

    # 3. Cuvinte majuscule din text (doar daca nu avem deja ceva)
    if not results:
        words = WORD_RE.findall(upper)
        for w in words:
            if w not in STOPWORDS and w not in MAJOR_TOKENS and len(w) >= 3:
                results.append(w)

    return list(dict.fromkeys(results))  # deduplicare pastrind ordinea


async def collect_telegram_symbols(db, hours: int = 12) -> List[Dict]:
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        signals = await db.telegram_signals.find({
            "posted_at": {"$gte": cutoff},
            "symbol": {"$exists": True, "$ne": None},
        }).sort("posted_at", -1).limit(200).to_list(length=200)

        seen: Set[str] = set()
        results = []
        for s in signals:
            sym = (s.get("symbol") or "").upper()
            if not sym or sym in seen or sym in MAJOR_TOKENS or sym in STOPWORDS:
                continue
            seen.add(sym)
            results.append({
                "symbol": sym,
                "source": "telegram",
                "direction": s.get("direction", "pump"),
                "score": float(s.get("composite_score") or 0),
                "mentions": 1,
            })
        logger.info(f"Telegram: {len(results)} simboluri")
        return results
    except Exception as e:
        logger.warning(f"Telegram collect error: {e}")
        return []


async def collect_reddit_symbols(client: httpx.AsyncClient) -> List[Dict]:
    seen: Set[str] = set()
    results = []
    for sub in SUBREDDITS:
        try:
            resp = await client.get(
                f"https://www.reddit.com/r/{sub}/new.rss?limit=25",
                headers={"User-Agent": "Mozilla/5.0 PumpRadar/2.0"},
                timeout=10,
            )
            if resp.status_code != 200:
                continue
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            root = ET.fromstring(resp.content)
            for entry in root.findall("atom:entry", ns):
                title_el = entry.find("atom:title", ns)
                content_el = entry.find("atom:content", ns)
                title = title_el.text if title_el is not None else ""
                content = content_el.text if content_el is not None else ""
                full_text = f"{title} {content}"
                low = full_text.lower()

                is_bullish = any(k in low for k in BULLISH_KW)
                is_bearish = any(k in low for k in BEARISH_KW)
                if not is_bullish and not is_bearish:
                    continue

                direction = "dump" if is_bearish and not is_bullish else "pump"
                symbols = extract_symbols_from_text(full_text)

                for sym in symbols[:2]:
                    if sym in seen:
                        continue
                    seen.add(sym)
                    results.append({
                        "symbol": sym,
                        "source": "reddit",
                        "direction": direction,
                        "score": 50.0,
                        "mentions": 1,
                    })
        except Exception as e:
            logger.debug(f"Reddit {sub} error: {e}")
        await asyncio.sleep(0.3)
    logger.info(f"Reddit: {len(results)} simboluri")
    return results


async def collect_cointelegraph_symbols(client: httpx.AsyncClient) -> List[Dict]:
    seen: Set[str] = set()
    results = []
    for feed_url in CT_FEEDS:
        try:
            resp = await client.get(feed_url, timeout=10)
            if resp.status_code != 200:
                continue
            root = ET.fromstring(resp.content)
            for item in root.findall(".//item"):
                title_el = item.find("title")
                desc_el = item.find("description")
                title = title_el.text if title_el is not None else ""
                desc = desc_el.text if desc_el is not None else ""
                full_text = f"{title} {desc}"
                low = full_text.lower()

                is_bullish = any(k in low for k in BULLISH_KW)
                is_bearish = any(k in low for k in BEARISH_KW)
                if not is_bullish and not is_bearish:
                    continue

                direction = "dump" if is_bearish and not is_bullish else "pump"
                symbols = extract_symbols_from_text(full_text)

                for sym in symbols[:2]:
                    if sym in seen:
                        continue
                    seen.add(sym)
                    results.append({
                        "symbol": sym,
                        "source": "cointelegraph",
                        "direction": direction,
                        "score": 45.0,
                        "mentions": 1,
                    })
        except Exception as e:
            logger.debug(f"Cointelegraph {feed_url} error: {e}")
    logger.info(f"Cointelegraph: {len(results)} simboluri")
    return results


async def collect_dexscreener_symbols(client: httpx.AsyncClient) -> List[Dict]:
    seen: Set[str] = set()
    results = []
    try:
        resp = await client.get(
            "https://api.dexscreener.com/token-boosts/top/v1",
            timeout=12,
        )
        if resp.status_code != 200:
            logger.debug(f"DexScreener boosts status: {resp.status_code}")
            return results
        items = resp.json() if isinstance(resp.json(), list) else []
        for item in items[:50]:
            sym = (item.get("tokenSymbol") or "").upper()
            chain = (item.get("chainId") or "").lower()
            token_address = item.get("tokenAddress") or ""
            if not sym or sym in seen or sym in MAJOR_TOKENS or sym in STOPWORDS:
                continue
            if len(sym) < 2:
                continue
            seen.add(sym)
            results.append({
                "symbol": sym,
                "source": "dexscreener",
                "direction": "pump",
                "score": 60.0,
                "token_address": token_address,
                "chain": chain,
                "pair_address": None,
                "mentions": 1,
            })
    except Exception as e:
        logger.debug(f"DexScreener boosts error: {e}")
    logger.info(f"DexScreener: {len(results)} simboluri")
    return results


async def collect_all_candidates(db) -> List[Dict]:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        results = await asyncio.gather(
            collect_telegram_symbols(db),
            collect_reddit_symbols(client),
            collect_cointelegraph_symbols(client),
            collect_dexscreener_symbols(client),
            return_exceptions=True,
        )

    all_raw = []
    for r in results:
        if isinstance(r, list):
            all_raw.extend(r)
        elif isinstance(r, Exception):
            logger.warning(f"Source error: {r}")

    merged: Dict[str, Dict] = {}
    for item in all_raw:
        sym = item["symbol"]
        if sym not in merged:
            merged[sym] = {
                "symbol": sym,
                "sources": [],
                "direction": item.get("direction", "pump"),
                "score": 0.0,
                "mentions": 0,
                "token_address": item.get("token_address"),
                "chain": item.get("chain"),
                "pair_address": item.get("pair_address"),
            }
        merged[sym]["sources"].append(item["source"])
        merged[sym]["score"] = max(merged[sym]["score"], item.get("score", 0))
        merged[sym]["mentions"] += 1
        if item.get("token_address"):
            merged[sym]["token_address"] = item["token_address"]
        if item.get("chain"):
            merged[sym]["chain"] = item["chain"]

    candidates = sorted(merged.values(), key=lambda x: x["mentions"], reverse=True)
    logger.info(f"Total candidati unici: {len(candidates)} din {len(all_raw)} semnale brute")
    candidates = await resolve_symbols_with_haiku(candidates)
    logger.info(f"Dupa Haiku resolution: {len(candidates)} candidati valizi")
    return candidates


async def resolve_symbols_with_haiku(candidates: List[Dict]) -> List[Dict]:
    """
    Foloseste Haiku sa curețe simbolurile brute:
    - Elimina noise (LAUNCHES, HITS, CEO etc.)
    - Corecteaza tickerele (JUPITER -> JUP, DOGWIFHAT -> WIF)
    - Adauga chain hint (solana/ethereum/bsc)
    Trimite batch-uri de 50 simboluri per apel.
    """
    import json
    import os
    import httpx as _httpx

    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY lipsa - skip Haiku resolution")
        return candidates

    system = "You are a crypto token identifier. Return ONLY valid JSON, no markdown, no explanation."

    resolved_map: Dict[str, Dict] = {}
    batch_size = 50

    symbols_list = [c["symbol"] for c in candidates]

    for i in range(0, len(symbols_list), batch_size):
        batch = symbols_list[i:i + batch_size]
        prompt = (
            f"From this list identify ONLY real crypto tokens. Remove common English words and noise.\n\n"
            f"Input: {', '.join(batch)}\n\n"
            f"Return JSON: {{\"tokens\": [{{\"input\": \"JUPITER\", \"symbol\": \"JUP\", \"chain\": \"solana\"}}]}}\n"
            f"Rules: Only include real crypto tokens. Correct ticker if needed (JUPITER->JUP, DOGWIFHAT->WIF). "
            f"Chain must be one of: solana, ethereum, bsc, unknown."
        )
        try:
            async with _httpx.AsyncClient(timeout=20) as client:
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": "claude-haiku-4-5-20251001",
                        "max_tokens": 1000,
                        "temperature": 0.0,
                        "system": system,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                )
            if resp.status_code != 200:
                logger.warning(f"Haiku resolution error: {resp.status_code}")
                continue
            content = resp.json().get("content") or []
            text = (content[0].get("text") or "").strip() if content else ""
            # Strip markdown
            if text.startswith("```"):
                text = "\n".join(l for l in text.splitlines() if not l.strip().startswith("```")).strip()
            parsed = json.loads(text)
            for token in (parsed.get("tokens") or []):
                inp = (token.get("input") or "").upper()
                sym = (token.get("symbol") or "").upper()
                chain = (token.get("chain") or "unknown").lower()
                if inp and sym:
                    resolved_map[inp] = {"symbol": sym, "chain": chain}
        except Exception as e:
            logger.warning(f"Haiku resolution batch error: {e}")
        await asyncio.sleep(0.3)

    logger.info(f"Haiku resolution: {len(resolved_map)} tokeni valizi din {len(symbols_list)} simboluri brute")

    # Actualizeaza candidatii cu simbolul corectat si chain-ul
    result = []
    for c in candidates:
        sym = c["symbol"]
        resolved = resolved_map.get(sym)
        if not resolved:
            # Eliminat de Haiku ca noise
            continue
        c["symbol"] = resolved["symbol"]
        if resolved["chain"] != "unknown" and not c.get("chain"):
            c["chain"] = resolved["chain"]
        result.append(c)

    return result
