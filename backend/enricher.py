"""
PumpRadar Enricher - Pas 3+4
Imbogateste candidatii cu date piata din GeckoTerminal + securitate.
"""
from __future__ import annotations
import os

import asyncio
import logging
from typing import Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

GECKOTERMINAL_BASE = "https://api.geckoterminal.com/api/v2"
GOPLUS_BASE = "https://api.gopluslabs.io/api/v1"

DEFI_API_KEY = "3ff957b3bddc4104adbe6c8f447866d0"
DEFI_GRAPHQL_URL = "https://public-api.de.fi/graphql"

HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "").strip()
ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "").strip()

GOPLUS_CHAIN_IDS = {
    "ethereum": "1", "eth": "1",
    "bsc": "56", "binance-smart-chain": "56",
    "polygon": "137", "arbitrum": "42161",
    "base": "8453", "avalanche": "43114",
    "optimism": "10", "solana": "solana",
}

GT_NETWORKS = ["solana", "eth", "bsc"]


def _sf(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


async def resolve_token_address(client: httpx.AsyncClient, symbol: str) -> Optional[Dict]:
    """Rezolva simbolul la token_address + chain via DexScreener search."""
    try:
        resp = await client.get(
            "https://api.dexscreener.com/latest/dex/search",
            params={"q": symbol},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        pairs = (resp.json().get("pairs") or [])
        if not pairs:
            return None
        # Filtreaza perechi cu simbolul exact
        exact = [
            p for p in pairs
            if (p.get("baseToken") or {}).get("symbol", "").upper() == symbol.upper()
        ]
        candidates = exact or pairs
        # Sorteaza dupa volum h24
        candidates.sort(
            key=lambda p: float((p.get("volume") or {}).get("h24") or 0),
            reverse=True,
        )
        best = candidates[0]
        base = best.get("baseToken") or {}
        # Mapeaza market data din DexScreener (fallback cand GT pica pe 429)
        _vol = best.get("volume") or {}
        _chg = best.get("priceChange") or {}
        _liq = best.get("liquidity") or {}
        _txh1 = (best.get("txns") or {}).get("h1") or {}
        _buys = _txh1.get("buys") or 0
        _sells = _txh1.get("sells") or 0
        _bs_ratio = (float(_buys) / float(_sells)) if _sells else (float(_buys) if _buys else None)
        try:
            _price = float(best.get("priceUsd")) if best.get("priceUsd") is not None else None
        except (TypeError, ValueError):
            _price = None
        _reserve = _liq.get("usd") or 0
        _vol_h24 = _vol.get("h24")
        market_ds = {
            "symbol": base.get("symbol", symbol).upper(),
            "network": best.get("chainId"),
            "dex": best.get("dexId") or "unknown",
            "pool_address": best.get("pairAddress"),
            "pool_url": best.get("url"),
            "token_address": base.get("address"),
            "price_usd": _price,
            "reserve_usd": _reserve,
            "fdv_usd": best.get("fdv"),
            "market_cap_usd": best.get("marketCap"),
            "volume_usd": {
                "m5": (_vol.get("m5")),
                "h1": (_vol.get("h1")),
                "h24": _vol_h24,
            },
            "price_change_pct": {
                "m5": _chg.get("m5"),
                "h1": _chg.get("h1"),
                "h24": _chg.get("h24"),
            },
            "transactions": {
                "h1_buys": int(_buys),
                "h1_sells": int(_sells),
                "h1_buy_sell_ratio": round(_bs_ratio, 3) if _bs_ratio is not None else None,
            },
            "volume_liquidity_ratio_h24": round(float(_vol_h24) / _reserve, 4) if (_reserve and _vol_h24) else 0,
            "source": "dexscreener",
        }
        return {
            "token_address": base.get("address"),
            "chain": best.get("chainId"),
            "pair_address": best.get("pairAddress"),
            "symbol": base.get("symbol", symbol).upper(),
            "market_ds": market_ds,
        }
    except Exception as e:
        logger.debug(f"DexScreener resolve error for {symbol}: {e}")
        return None


async def fetch_geckoterminal_by_address(client: httpx.AsyncClient, network: str, token_address: str, symbol: str) -> Optional[Dict]:
    """Fetch date GT dupa token address - date corecte garantat."""
    try:
        resp = await client.get(
            f"{GECKOTERMINAL_BASE}/networks/{network}/tokens/{token_address}/pools",
            params={"page": 1},
            timeout=12,
        )
        if resp.status_code != 200:
            return None
        pools = (resp.json().get("data") or [])
        if not pools:
            return None
        # Cel mai activ pool dupa volum h1
        best = max(
            pools[:10],
            key=lambda p: _sf((p.get("attributes") or {}).get("volume_usd", {}).get("h1") or 0) or 0,
        )
        attrs = best.get("attributes") or {}
        rels = best.get("relationships") or {}
        dex = (rels.get("dex") or {}).get("data", {}).get("id") or "unknown"
        volume = attrs.get("volume_usd") or {}
        changes = attrs.get("price_change_percentage") or {}
        tx = attrs.get("transactions") or {}
        h1_tx = tx.get("h1") or {}
        h24_tx = tx.get("h24") or {}
        buys_h1 = int(h1_tx.get("buys") or 0)
        sells_h1 = int(h1_tx.get("sells") or 0)
        buys_h24 = int(h24_tx.get("buys") or 0)
        sells_h24 = int(h24_tx.get("sells") or 0)
        reserve_usd = _sf(attrs.get("reserve_in_usd"))
        vol_h24 = _sf(volume.get("h24"))
        vol_h1 = _sf(volume.get("h1"))
        return {
            "symbol": symbol,
            "network": network,
            "dex": dex,
            "pool_address": attrs.get("address"),
            "pool_url": f"https://www.geckoterminal.com/{network}/pools/{attrs.get('address')}",
            "token_address": token_address,
            "price_usd": _sf(attrs.get("base_token_price_usd")),
            "reserve_usd": reserve_usd,
            "fdv_usd": _sf(attrs.get("fdv_usd")),
            "market_cap_usd": _sf(attrs.get("market_cap_usd")),
            "volume_usd": {
                "m5": _sf(volume.get("m5")),
                "h1": vol_h1,
                "h24": vol_h24,
            },
            "price_change_pct": {
                "m5": _sf(changes.get("m5")),
                "h1": _sf(changes.get("h1")),
                "h24": _sf(changes.get("h24")),
            },
            "transactions": {
                "h1_buys": buys_h1,
                "h1_sells": sells_h1,
                "h24_buys": buys_h24,
                "h24_sells": sells_h24,
                "h1_buy_sell_ratio": round(buys_h1 / max(sells_h1, 1), 3),
                "h24_buy_sell_ratio": round(buys_h24 / max(sells_h24, 1), 3),
            },
            "volume_liquidity_ratio_h24": round(vol_h24 / reserve_usd, 4) if reserve_usd else 0,
            "source": "geckoterminal_address",
        }
    except Exception as e:
        logger.debug(f"GT address lookup error for {symbol}: {e}")
        return None


async def fetch_geckoterminal_symbol(client: httpx.AsyncClient, symbol: str) -> Optional[Dict]:
    """Cauta un simbol pe GeckoTerminal prin search, selecteaza pool-ul cu volum h1 maxim."""
    try:
        resp = await client.get(
            f"{GECKOTERMINAL_BASE}/search/pools",
            params={"query": symbol, "page": 1},
            timeout=12,
        )
        if resp.status_code != 200:
            return None
        pools = (resp.json().get("data") or [])
        if not pools:
            return None
        # Filtrare: simbolul exact + lichiditate minima
        exact = [p for p in pools[:10] if (p.get("attributes") or {}).get("name", "").split("/")[0].strip().upper() == symbol.upper()]
        candidates = exact if exact else pools[:10]
        candidates = [p for p in candidates if (_sf((p.get("attributes") or {}).get("reserve_in_usd")) or 0) >= 5000]
        if not candidates:
            return None
        # Selectam pool-ul cu volum h1 maxim
        best = max(candidates, key=lambda p: (_sf((p.get("attributes") or {}).get("volume_usd", {}).get("h1")) or 0))
        attrs = best.get("attributes") or {}
        rels = best.get("relationships") or {}
        network = (rels.get("network") or {}).get("data", {}).get("id") or "unknown"
        dex = (rels.get("dex") or {}).get("data", {}).get("id") or "unknown"
        volume = attrs.get("volume_usd") or {}
        changes = attrs.get("price_change_percentage") or {}
        tx = attrs.get("transactions") or {}
        h1_tx = tx.get("h1") or {}
        h24_tx = tx.get("h24") or {}
        buys_h1 = int(h1_tx.get("buys") or 0)
        sells_h1 = int(h1_tx.get("sells") or 0)
        buys_h24 = int(h24_tx.get("buys") or 0)
        sells_h24 = int(h24_tx.get("sells") or 0)
        reserve_usd = _sf(attrs.get("reserve_in_usd"))
        vol_h24 = _sf(volume.get("h24"))
        vol_h1 = _sf(volume.get("h1"))
        return {
            "symbol": symbol,
            "network": network,
            "dex": dex,
            "pool_address": attrs.get("address"),
            "pool_url": f"https://www.geckoterminal.com/{network}/pools/{attrs.get('address')}",
            "token_address": (attrs.get("name") or "").split("/")[0].strip(),
            "price_usd": _sf(attrs.get("base_token_price_usd")),
            "reserve_usd": reserve_usd,
            "fdv_usd": _sf(attrs.get("fdv_usd")),
            "market_cap_usd": _sf(attrs.get("market_cap_usd")),
            "volume_usd": {"m5": _sf(volume.get("m5")), "h1": vol_h1, "h24": vol_h24},
            "price_change_pct": {"m5": _sf(changes.get("m5")), "h1": _sf(changes.get("h1")), "h24": _sf(changes.get("h24"))},
            "transactions": {
                "h1_buys": buys_h1, "h1_sells": sells_h1,
                "h24_buys": buys_h24, "h24_sells": sells_h24,
                "h1_buy_sell_ratio": round(buys_h1 / max(sells_h1, 1), 3),
                "h24_buy_sell_ratio": round(buys_h24 / max(sells_h24, 1), 3),
            },
            "volume_liquidity_ratio_h24": round(vol_h24 / reserve_usd, 4) if reserve_usd else 0,
            "source": "geckoterminal_search",
        }
    except Exception as e:
        logger.debug(f"GT search error for {symbol}: {e}")
        return None

async def fetch_geckoterminal_trending(client: httpx.AsyncClient) -> List[Dict]:
    """Fetch trending pools din GeckoTerminal pentru toate networkurile."""
    results = []
    for network in GT_NETWORKS:
        for mode in ["trending_pools", "new_pools"]:
            try:
                resp = await client.get(
                    f"{GECKOTERMINAL_BASE}/networks/{network}/{mode}",
                    params={"include": "base_token,dex"},
                    timeout=15,
                )
                if resp.status_code != 200:
                    continue
                payload = resp.json()
                pools = payload.get("data") or []
                included = payload.get("included") or []
                token_index = {
                    item.get("id"): item.get("attributes") or {}
                    for item in included if item.get("type") == "token"
                }
                dex_index = {
                    item.get("id"): item.get("attributes") or {}
                    for item in included if item.get("type") == "dex"
                }
                for pool in pools[:20]:
                    attrs = pool.get("attributes") or {}
                    rels = pool.get("relationships") or {}
                    base_id = ((rels.get("base_token") or {}).get("data") or {}).get("id")
                    dex_id = ((rels.get("dex") or {}).get("data") or {}).get("id")
                    base = token_index.get(base_id, {})
                    dex = dex_index.get(dex_id, {})
                    volume = attrs.get("volume_usd") or {}
                    changes = attrs.get("price_change_percentage") or {}
                    tx = attrs.get("transactions") or {}
                    h1_tx = tx.get("h1") or {}
                    h24_tx = tx.get("h24") or {}
                    buys_h1 = int(h1_tx.get("buys") or 0)
                    sells_h1 = int(h1_tx.get("sells") or 0)
                    buys_h24 = int(h24_tx.get("buys") or 0)
                    sells_h24 = int(h24_tx.get("sells") or 0)
                    reserve_usd = _sf(attrs.get("reserve_in_usd"))
                    vol_h24 = _sf(volume.get("h24"))
                    sym = (base.get("symbol") or "").upper()
                    if not sym:
                        continue
                    results.append({
                        "symbol": sym,
                        "network": network,
                        "dex": dex.get("name") or dex_id,
                        "pool_address": attrs.get("address"),
                        "pool_url": f"https://www.geckoterminal.com/{network}/pools/{attrs.get('address')}",
                        "token_address": base.get("address"),
                        "price_usd": _sf(attrs.get("base_token_price_usd")),
                        "reserve_usd": reserve_usd,
                        "fdv_usd": _sf(attrs.get("fdv_usd")),
                        "market_cap_usd": _sf(attrs.get("market_cap_usd")),
                        "volume_usd": {
                            "m5": _sf(volume.get("m5")),
                            "h1": _sf(volume.get("h1")),
                            "h24": vol_h24,
                        },
                        "price_change_pct": {
                            "m5": _sf(changes.get("m5")),
                            "h1": _sf(changes.get("h1")),
                            "h24": _sf(changes.get("h24")),
                        },
                        "transactions": {
                            "h1_buys": buys_h1,
                            "h1_sells": sells_h1,
                            "h24_buys": buys_h24,
                            "h24_sells": sells_h24,
                            "h1_buy_sell_ratio": round(buys_h1 / max(sells_h1, 1), 3),
                            "h24_buy_sell_ratio": round(buys_h24 / max(sells_h24, 1), 3),
                        },
                        "volume_liquidity_ratio_h24": round(vol_h24 / reserve_usd, 4) if reserve_usd else 0,
                        "mode": mode,
                        "source": f"geckoterminal_{mode}",
                    })
            except Exception as e:
                logger.debug(f"GT {network}/{mode} error: {e}")
        await asyncio.sleep(0.5)
    logger.info(f"GeckoTerminal trending: {len(results)} pools")
    return results


async def fetch_goplus_security(client: httpx.AsyncClient, chain: str, token_address: str) -> Dict:
    """Fetch securitate token din GoPlus."""
    try:
        if chain == "solana":
            resp = await client.get(
                f"{GOPLUS_BASE}/solana/token_security",
                params={"contract_addresses": token_address},
                timeout=12,
            )
        else:
            chain_id = GOPLUS_CHAIN_IDS.get(chain.lower())
            if not chain_id:
                return {"available": False}
            resp = await client.get(
                f"{GOPLUS_BASE}/token_security/{chain_id}",
                params={"contract_addresses": token_address},
                timeout=12,
            )
        if resp.status_code != 200:
            return {"available": False}
        result = resp.json().get("result") or {}
        data = result.get(token_address) or result.get(token_address.lower()) or {}
        if not data and len(result) == 1:
            data = next(iter(result.values()))

        red_flags = []
        if str(data.get("is_honeypot", "0")) == "1":
            red_flags.append("honeypot")
        if str(data.get("is_blacklisted", "0")) == "1":
            red_flags.append("blacklist")
        if str(data.get("cannot_sell_all", "0")) == "1":
            red_flags.append("cannot_sell_all")
        buy_tax = _sf(data.get("buy_tax", 0))
        sell_tax = _sf(data.get("sell_tax", 0))
        if max(buy_tax, sell_tax) >= 10:
            red_flags.append("high_tax")

        return {
            "available": True,
            "red_flags": red_flags,
            "buy_tax": buy_tax,
            "sell_tax": sell_tax,
            "is_open_source": str(data.get("is_open_source", "0")) == "1",
            "owner_address": data.get("owner_address"),
            "source": "GoPlus",
        }
    except Exception as e:
        logger.debug(f"GoPlus error {chain}/{token_address}: {e}")
        return {"available": False}


_solana_exchange_identity_cache: Dict[str, bool] = {}


async def _is_solana_exchange_wallet(client: httpx.AsyncClient, address: str) -> bool:
    """Uses Helius Wallet Identity API to check if an address is a known
    CEX wallet. Replaces unreliable substring matching - Solana addresses
    are base58 strings and never literally contain 'binance' etc, so the
    previous check almost never matched anything real."""
    if not address:
        return False
    if address in _solana_exchange_identity_cache:
        return _solana_exchange_identity_cache[address]
    if not HELIUS_API_KEY:
        return False
    try:
        resp = await client.get(
            f"https://api.helius.xyz/v1/wallet/{address}/identity",
            params={"api-key": HELIUS_API_KEY},
            timeout=8,
        )
        if resp.status_code != 200:
            _solana_exchange_identity_cache[address] = False
            return False
        data = resp.json() or {}
        category = str(data.get("category", "")).upper()
        is_cex = category == "CEX"
        _solana_exchange_identity_cache[address] = is_cex
        return is_cex
    except Exception:
        _solana_exchange_identity_cache[address] = False
        return False


_solana_pools_cache: Dict[str, set] = {}


async def _get_solana_token_pools(client: httpx.AsyncClient, token_address: str) -> set:
    """Returns a set of lowercase DEX pool addresses for this token on Solana,
    via GeckoTerminal. A transfer TO one of these pools means the whale SOLD
    the token (swapped it away) - without this check, sells via DEX swap were
    being miscounted as buys, since only known-CEX destinations were excluded."""
    if token_address in _solana_pools_cache:
        return _solana_pools_cache[token_address]
    pools: set = set()
    try:
        r = await client.get(
            f"https://api.geckoterminal.com/api/v2/networks/solana/tokens/{token_address}/pools",
            timeout=10,
        )
        if r.status_code == 200:
            d = r.json()
            for item in d.get("data", [])[:5]:
                addr = (item.get("attributes", {}) or {}).get("address", "")
                if addr:
                    pools.add(addr.lower())
    except Exception:
        pass
    _solana_pools_cache[token_address] = pools
    return pools


async def fetch_whale_activity_helius(client: httpx.AsyncClient, token_address: str, symbol: str) -> Dict:
    """Detecteaza activitate whale pentru tokeni Solana via Helius."""
    if not HELIUS_API_KEY or not token_address:
        return {"available": False, "whale_score": 0}
    try:
        resp = await client.get(
            f"https://api.helius.xyz/v0/addresses/{token_address}/transactions",
            params={"api-key": HELIUS_API_KEY, "limit": 30, "type": "TRANSFER"},
            timeout=12,
        )
        if resp.status_code != 200:
            return {"available": False, "whale_score": 0}
        txs = resp.json()
        if not isinstance(txs, list):
            return {"available": False, "whale_score": 0}

        large_moves = 0
        unique_buyers = set()
        unique_sellers = set()
        pools = await _get_solana_token_pools(client, token_address)

        for tx in txs:
            for transfer in tx.get("tokenTransfers", []):
                amount = float(transfer.get("tokenAmount") or 0)
                if amount < 1000:
                    continue
                to_wallet = transfer.get("toUserAccount", "")
                from_wallet = transfer.get("fromUserAccount", "")
                to_exchange = await _is_solana_exchange_wallet(client, to_wallet)
                to_pool = to_wallet.lower() in pools
                if to_exchange or to_pool:
                    unique_sellers.add(from_wallet)
                else:
                    unique_buyers.add(to_wallet)
                large_moves += 1

        accumulation = len(unique_buyers) >= 3 and len(unique_buyers) > len(unique_sellers) * 1.5
        dump_risk = len(unique_sellers) >= 3
        whale_score = min(100, large_moves * 5 + len(unique_buyers) * 8 + (20 if accumulation else 0) - (25 if dump_risk else 0))

        return {
            "available": True,
            "whale_score": max(0, whale_score),
            "accumulation_detected": accumulation,
            "dump_risk": dump_risk,
            "large_moves": large_moves,
            "unique_buyers": len(unique_buyers),
            "unique_sellers": len(unique_sellers),
            "chain": "solana",
        }
    except Exception as e:
        logger.debug(f"Helius whale error for {symbol}: {e}")
        return {"available": False, "whale_score": 0}


async def fetch_whale_activity_etherscan(client: httpx.AsyncClient, token_address: str, symbol: str) -> Dict:
    """Detecteaza activitate whale pentru tokeni EVM via Etherscan."""
    if not ETHERSCAN_API_KEY or not token_address or not token_address.startswith("0x"):
        return {"available": False, "whale_score": 0}
    try:
        resp = await client.get(
            "https://api.etherscan.io/v2/api",
            params={
                "chainid": 1,
                "module": "account",
                "action": "tokentx",
                "contractaddress": token_address,
                "sort": "desc",
                "page": 1,
                "offset": 30,
                "apikey": ETHERSCAN_API_KEY,
            },
            timeout=12,
        )
        if resp.status_code != 200:
            return {"available": False, "whale_score": 0}
        data = resp.json()
        if data.get("status") != "1":
            return {"available": False, "whale_score": 0}

        txs = data.get("result", [])
        KNOWN_EXCHANGE_ADDRS = {
            "0x28c6c06298d514db089934071355e5743bf21d60",
            "0x21a31ee1afc51d94c2efccaa2092ad1028285549",
        }
        large_moves = 0
        unique_buyers = set()
        unique_sellers = set()

        for tx in txs:
            decimals = int(tx.get("tokenDecimal", 18))
            amount = float(tx.get("value", 0)) / (10 ** decimals)
            if amount < 1000:
                continue
            from_addr = (tx.get("from") or "").lower()
            to_addr = (tx.get("to") or "").lower()
            to_exchange = to_addr in KNOWN_EXCHANGE_ADDRS
            if to_exchange:
                unique_sellers.add(from_addr)
            else:
                unique_buyers.add(to_addr)
            large_moves += 1

        accumulation = len(unique_buyers) >= 3 and len(unique_buyers) > len(unique_sellers) * 1.5
        dump_risk = len(unique_sellers) >= 2
        whale_score = min(100, large_moves * 5 + len(unique_buyers) * 8 + (20 if accumulation else 0) - (25 if dump_risk else 0))

        return {
            "available": True,
            "whale_score": max(0, whale_score),
            "accumulation_detected": accumulation,
            "dump_risk": dump_risk,
            "large_moves": large_moves,
            "unique_buyers": len(unique_buyers),
            "unique_sellers": len(unique_sellers),
            "chain": "ethereum",
        }
    except Exception as e:
        logger.debug(f"Etherscan whale error for {symbol}: {e}")
        return {"available": False, "whale_score": 0}


async def fetch_defi_rekt(client: httpx.AsyncClient, symbol: str) -> Dict:
    """Verifica daca tokenul e in baza de date REKT a De.Fi (hack/scam cunoscut)."""
    try:
        query = """{
          rekts(searchText: "%s", pageNumber: 1, pageSize: 3) {
            projectName
            fundsLost
            date
            category
            issueType
          }
        }""" % symbol.upper()
        resp = await client.post(
            DEFI_GRAPHQL_URL,
            headers={"X-Api-Key": DEFI_API_KEY, "Content-Type": "application/json"},
            json={"query": query},
            timeout=10,
        )
        if resp.status_code != 200:
            return {"available": False, "is_rekt": False, "rekt_data": []}
        data = resp.json()
        rekts = (data.get("data") or {}).get("rekts") or []
        if not isinstance(rekts, list):
            rekts = []
        matched = [r for r in rekts if (
    symbol.upper() == (r.get("projectName") or "").upper().strip() or
    symbol.upper() == (r.get("projectName") or "").upper().split()[0].strip()
)]
        return {
            "available": True,
            "is_rekt": len(matched) > 0,
            "rekt_data": matched[:2],
            "funds_lost": sum(float(r.get("fundsLost") or 0) for r in matched),
        }
    except Exception as e:
        logger.debug(f"De.Fi rekt check error for {symbol}: {e}")
        return {"available": False, "is_rekt": False, "rekt_data": []}


async def enrich_candidate(client: httpx.AsyncClient, candidate: Dict) -> Optional[Dict]:
    symbol = candidate["symbol"]
    token_address = candidate.get("token_address")
    chain = candidate.get("chain") or "ethereum"

    # Resolve token address via DexScreener daca nu avem
    market_ds = None
    if not token_address:
        resolved = await resolve_token_address(client, symbol)
        if resolved:
            token_address = resolved.get("token_address")
            chain = resolved.get("chain") or chain
            market_ds = resolved.get("market_ds")
            logger.debug(f"{symbol} rezolvat: {token_address} pe {chain}")

    # Fetch market data GT by address (date corecte) sau search fallback
    market = None
    if token_address and len(str(token_address)) > 10:
        chain_map = {
            "ethereum": "eth", "eth": "eth",
            "bsc": "bsc", "binance-smart-chain": "bsc",
            "solana": "solana", "sol": "solana",
            "arbitrum": "arbitrum", "arbitrum-one": "arbitrum",
            "polygon": "polygon_pos", "polygon-pos": "polygon_pos",
            "base": "base", "avalanche": "avax",
            "optimism": "optimism",
        }
        gt_network = chain_map.get(chain.lower(), chain.lower())
        market = await fetch_geckoterminal_by_address(client, gt_network, token_address, symbol)
    if not market:
        market = await fetch_geckoterminal_symbol(client, symbol)
    if not market and market_ds and market_ds.get("price_usd") is not None:
        # GT a picat (probabil 429) - folosim datele DexScreener deja obtinute
        ds_reserve = market_ds.get("reserve_usd") or 0
        if ds_reserve >= 5000:
            logger.info(f"{symbol} enriched via DexScreener fallback: ${market_ds.get('price_usd')}")
            security = {"available": False, "red_flags": []}
            ds_addr = market_ds.get("token_address") or token_address
            ds_net = (market_ds.get("network") or chain or "").lower()
            defi_rekt = await fetch_defi_rekt(client, symbol)
            if defi_rekt.get("is_rekt"):
                security["red_flags"] = ["defi_rekt_database"]
            whale = {"available": False, "whale_score": 0}
            if ds_addr:
                if ds_net == "solana":
                    whale = await fetch_whale_activity_helius(client, ds_addr, symbol)
                elif ds_net in ("ethereum", "eth"):
                    whale = await fetch_whale_activity_etherscan(client, ds_addr, symbol)
                if whale.get("accumulation_detected"):
                    logger.info(f"{symbol} whale accumulation detectat - score={whale.get('whale_score')}")
                if whale.get("dump_risk"):
                    security["red_flags"] = list(set((security.get("red_flags") or []) + ["whale_dump_risk"]))
            return {
                **candidate,
                "market": market_ds,
                "security": security,
                "defi_rekt": defi_rekt,
                "enriched": True,
                "network": market_ds.get("network") or chain,
                "token_address": ds_addr,
                "pool_address": market_ds.get("pool_address"),
                "pool_url": market_ds.get("pool_url"),
                "price_usd": market_ds.get("price_usd"),
                "reserve_usd": market_ds.get("reserve_usd"),
                "volume_h24": (market_ds.get("volume_usd") or {}).get("h24"),
                "price_change_h1": (market_ds.get("price_change_pct") or {}).get("h1"),
                "price_change_h24": (market_ds.get("price_change_pct") or {}).get("h24"),
                "buy_sell_ratio_h1": (market_ds.get("transactions") or {}).get("h1_buy_sell_ratio"),
                "red_flags": security.get("red_flags") or [],
                "whale": whale,
            }
    if not market:
        market_cg = await fetch_coingecko_fallback(client, symbol)
        if market_cg:
            return {
                **candidate,
                "market": market_cg,
                "security": {"available": False, "red_flags": []},
                "defi_rekt": {"is_rekt": False},
                "enriched": True,
                "network": "cex",
                "token_address": market_cg.get("token_address"),
                "pool_address": None,
                "pool_url": market_cg.get("pool_url"),
                "price_usd": market_cg.get("price_usd"),
                "reserve_usd": market_cg.get("reserve_usd"),
                "volume_h24": market_cg.get("volume_h24"),
                "price_change_h1": market_cg.get("price_change_h1"),
                "price_change_h24": market_cg.get("price_change_h24"),
                "buy_sell_ratio_h1": None,
                "red_flags": [],
                "whale": {"available": False, "whale_score": 0},
            }
        logger.debug(f"Fara date GT pentru {symbol}")
        return None
    if market.get("reserve_usd", 0) < 5000:
        logger.debug(f"{symbol} sub lichiditate minima")
        return None

    security = {"available": False, "red_flags": []}
    addr = token_address or market.get("token_address")
    net = market.get("network") or chain
    if addr and len(str(addr)) > 10:
        security = await fetch_goplus_security(client, net, addr)

    defi_rekt = await fetch_defi_rekt(client, symbol)
    if defi_rekt.get("is_rekt"):
        security["red_flags"] = list(set((security.get("red_flags") or []) + ["defi_rekt_database"]))
        logger.info(f"{symbol} gasit in De.Fi REKT database")

    whale = {"available": False, "whale_score": 0}
    if addr:
        net_lower = (market.get("network") or chain or "").lower()
        if net_lower == "solana":
            whale = await fetch_whale_activity_helius(client, addr, symbol)
        elif net_lower in ("ethereum", "eth"):
            whale = await fetch_whale_activity_etherscan(client, addr, symbol)
        if whale.get("accumulation_detected"):
            logger.info(f"{symbol} whale accumulation detectat - score={whale.get('whale_score')}")
        if whale.get("dump_risk"):
            security["red_flags"] = list(set((security.get("red_flags") or []) + ["whale_dump_risk"]))

    return {
        **candidate,
        "market": market,
        "security": security,
        "defi_rekt": defi_rekt,
        "enriched": True,
        "network": market.get("network") or chain,
        "token_address": addr or token_address,
        "pool_address": market.get("pool_address"),
        "pool_url": market.get("pool_url"),
        "price_usd": market.get("price_usd"),
        "reserve_usd": market.get("reserve_usd"),
        "volume_h24": (market.get("volume_usd") or {}).get("h24"),
        "price_change_h1": (market.get("price_change_pct") or {}).get("h1"),
        "price_change_h24": (market.get("price_change_pct") or {}).get("h24"),
        "buy_sell_ratio_h1": (market.get("transactions") or {}).get("h1_buy_sell_ratio"),
        "red_flags": security.get("red_flags") or [],
        "whale": whale,
    }

async def enrich_all_candidates(candidates: List[Dict], trending_pools: List[Dict]) -> List[Dict]:
    """
    Pas 3+4: imbogateste candidatii.
    Filtreaza inainte de GT pentru a evita rate limiting.
    """
    # Adauga trending pools ca candidati prioritari
    existing_symbols = {c["symbol"] for c in candidates}
    for pool in trending_pools:
        sym = pool["symbol"]
        if sym not in existing_symbols:
            candidates.append({
                "symbol": sym,
                "sources": [pool.get("source", "geckoterminal")],
                "direction": "pump",
                "score": 60.0,
                "mentions": 1,
                "token_address": pool.get("token_address"),
                "chain": pool.get("network"),
                "pair_address": pool.get("pool_address"),
                "gt_pool": pool,
            })

    # Filtreaza candidatii - trimite la GT doar cei relevanti
    priority_sources = {"telegram", "dexscreener"}
    filtered = []
    for c in candidates:
        sources = set(c.get("sources") or [])
        mentions = c.get("mentions", 1)
        # Prioritate: multi-sursa, telegram, dexscreener, sau score mare
        if (
            mentions >= 2 or
            sources.intersection(priority_sources) or
            c.get("score", 0) >= 55 or
            c.get("token_address")
        ):
            filtered.append(c)

    logger.info(f"Candidati filtrati pentru enrichment: {len(filtered)} din {len(candidates)}")

    # Enrich in paralel cu rate limiting
    enriched = []
    async with httpx.AsyncClient(follow_redirects=True) as client:
        semaphore = asyncio.Semaphore(2)

        async def enrich_with_sem(c):
            async with semaphore:
                result = await enrich_candidate(client, c)
                await asyncio.sleep(2.5)
                return result

        results = await asyncio.gather(
            *[enrich_with_sem(c) for c in filtered[:40]],
            return_exceptions=True,
        )

    n_none = 0
    n_exc = 0
    for r in results:
        if isinstance(r, Exception):
            n_exc += 1
        elif isinstance(r, dict) and r.get("enriched"):
            enriched.append(r)
        else:
            n_none += 1
    logger.info(
        f"ENRICH STATS: {len(enriched)} enriched | {n_none} fara date/lichiditate | "
        f"{n_exc} exceptii | din {len(filtered)} filtrati (limit 120)"
    )
    return enriched


_cg_semaphore = None

async def fetch_coingecko_fallback(client: httpx.AsyncClient, symbol: str) -> Optional[Dict]:
    """Fallback CoinGecko pentru tokenuri CEX fara pool DEX"""
    global _cg_semaphore
    if _cg_semaphore is None:
        import asyncio
        _cg_semaphore = asyncio.Semaphore(3)
    async with _cg_semaphore:
        await asyncio.sleep(0.5)
    try:
        # Search by symbol
        search_url = f"https://api.coingecko.com/api/v3/search?query={symbol}"
        r = await client.get(search_url, timeout=8)
        if r.status_code != 200:
            return None
        data = r.json()
        coins = data.get("coins", [])
        if not coins:
            return None
        # Gasim cel mai relevant coin
        coin = next((c for c in coins if c.get("symbol", "").upper() == symbol.upper()), coins[0])
        coin_id = coin.get("id")
        if not coin_id:
            return None
        # Fetch market data
        market_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}?localization=false&tickers=false&community_data=false&developer_data=false"
        r2 = await client.get(market_url, timeout=10)
        if r2.status_code != 200:
            return None
        md = r2.json()
        mdata = md.get("market_data", {})
        price = mdata.get("current_price", {}).get("usd", 0)
        vol = mdata.get("total_volume", {}).get("usd", 0)
        h1 = mdata.get("price_change_percentage_1h_in_currency", {}).get("usd", 0)
        h24 = mdata.get("price_change_percentage_24h", 0)
        mcap = mdata.get("market_cap", {}).get("usd", 0)
        if not price:
            return None
        logger.info(f"{symbol} enriched via CoinGecko fallback: ${price}")
        return {
            "price_usd": price,
            "volume_h24": vol,
            "price_change_h1": h1 or 0,
            "price_change_h24": h24 or 0,
            "market_cap": mcap,
            "reserve_usd": mcap or vol,
            "network": "cex",
            "pool_url": f"https://www.coingecko.com/en/coins/{coin_id}",
            "pool_address": None,
            "token_address": coin_id,
            "buy_sell_ratio_h1": None,
            "source": "coingecko",
            "coingecko_id": coin_id,
        }
    except Exception as e:
        logger.debug(f"CoinGecko fallback error for {symbol}: {e}")
        return None
