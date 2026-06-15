"""
onchain_gecko_ingest.py - GeckoTerminal-based ingestion for BSC + Solana.

Polls the free GeckoTerminal public API `/networks/{net}/new_pools` for newly
created pools and writes `pair_created` documents into the SAME `onchain_events`
collection used by the WSS ingester, so they flow through the same enrichment
worker, the same API, and the same page.

Why this exists: ETH is covered by direct WSS (onchain_ingest.py). For BSC/Solana
a free WSS node would cost extra; GeckoTerminal's public API is free (30 req/min,
updated every ~30s) and returns the pool name, symbols, price and liquidity.

NEW standalone process. Writer only. Does NOT touch protected files.

Notes:
- BSC is EVM -> the existing enrichment worker scores it correctly (GoPlus).
- Solana has a different security model -> the worker leaves it as NO_DATA for now
  (pairs still flow with symbol + price); Solana-specific scoring is a follow-up.

Run with the backend venv:
    /srv/data/pump_radar/backend/.venv/bin/python onchain_gecko_ingest.py
Env:
    ONCHAIN_MONGO_URL   (default mongodb://localhost:27017)
"""
import asyncio
import logging
import os
from datetime import datetime, timezone

import httpx
from motor.motor_asyncio import AsyncIOMotorClient

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("onchain_gecko_ingest")

GT = "https://api.geckoterminal.com/api/v2"
NETWORKS = ["bsc", "solana"]  # eth already covered by WSS ingester
POLL_SECONDS = 30
BASE_SYMBOLS = {"WETH", "WBNB", "BNB", "USDT", "USDC", "BUSD", "DAI", "SOL", "WSOL", "USDH", "USDD", "FDUSD"}
MONGO_URL = os.environ.get("ONCHAIN_MONGO_URL", "mongodb://localhost:27017")


def parse_dt(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def norm_addr(net, addr):
    if not addr:
        return None
    return addr.lower() if net != "solana" else addr


async def poll_network(http, coll, net):
    r = await http.get(
        f"{GT}/networks/{net}/new_pools",
        params={"include": "base_token,quote_token,dex"},
        headers={"accept": "application/json"},
    )
    if r.status_code != 200:
        log.warning("[%s] gecko HTTP %s", net, r.status_code)
        return 0
    payload = r.json()

    tokens, dexes = {}, {}
    for inc in payload.get("included", []):
        if inc.get("type") == "token":
            tokens[inc.get("id")] = inc.get("attributes", {})
        elif inc.get("type") == "dex":
            dexes[inc.get("id")] = inc.get("attributes", {})

    inserted = 0
    for pool in payload.get("data", []):
        a = pool.get("attributes", {})
        rel = pool.get("relationships", {})
        pool_addr = a.get("address")
        if not pool_addr:
            continue

        bt_id = (rel.get("base_token", {}).get("data") or {}).get("id")
        qt_id = (rel.get("quote_token", {}).get("data") or {}).get("id")
        dex_id = (rel.get("dex", {}).get("data") or {}).get("id")
        bt, qt = tokens.get(bt_id, {}), tokens.get(qt_id, {})
        bt_sym, qt_sym = (bt.get("symbol") or "").upper(), (qt.get("symbol") or "").upper()
        bt_addr, qt_addr = bt.get("address"), qt.get("address")

        # the new token is the side that is NOT a known base/quote currency
        if qt_sym in BASE_SYMBOLS and bt_sym not in BASE_SYMBOLS:
            token_addr, token_sym, base_addr, base_sym = bt_addr, bt_sym, qt_addr, qt_sym
        elif bt_sym in BASE_SYMBOLS and qt_sym not in BASE_SYMBOLS:
            token_addr, token_sym, base_addr, base_sym = qt_addr, qt_sym, bt_addr, bt_sym
        else:
            token_addr, token_sym, base_addr, base_sym = bt_addr, bt_sym, qt_addr, qt_sym
        if not token_addr:
            continue

        dex_name = dexes.get(dex_id, {}).get("name") or dex_id or "unknown"
        doc = {
            "event_type": "pair_created",
            "chain": net,
            "block_number": None,
            "block_time": parse_dt(a.get("pool_created_at")),
            "tx_hash": f"gecko:{pool_addr}",
            "log_index": 0,
            "dex": dex_name,
            "factory": None,
            "token_address": norm_addr(net, token_addr),
            "base_token": norm_addr(net, base_addr),
            "pair_address": norm_addr(net, pool_addr),
            "token_symbol": token_sym or None,
            "base_symbol": base_sym or None,
            "data": {
                "token0": None, "token1": None, "pair_index": None,
                "fee_tier": None, "init_symbol": token_sym or None, "init_decimals": None,
            },
            "gecko": {
                "name": a.get("name"),
                "price_usd": a.get("base_token_price_usd"),
                "reserve_usd": a.get("reserve_in_usd"),
                "fdv_usd": a.get("fdv_usd"),
            },
            "ingested_at": datetime.now(timezone.utc),
            "source": "gecko",
            "enriched": False,
            "enrichment": None,
            "scores": {"early": None, "threat": None},
        }
        res = await coll.update_one(
            {"chain": net, "tx_hash": doc["tx_hash"], "log_index": 0},
            {"$setOnInsert": doc},
            upsert=True,
        )
        if res.upserted_id is not None:
            inserted += 1
            log.info("[%s] new pool %s %s/%s dex=%s", net, pool_addr, token_sym, base_sym, dex_name)
    return inserted


async def main():
    client = AsyncIOMotorClient(MONGO_URL)
    coll = client["pumpradar"]["onchain_events"]
    log.info("gecko ingest starting | networks=%s poll=%ds", ",".join(NETWORKS), POLL_SECONDS)
    async with httpx.AsyncClient(timeout=20) as http:
        while True:
            for net in NETWORKS:
                try:
                    await poll_network(http, coll, net)
                except Exception as e:
                    log.warning("[%s] poll error: %s", net, e)
                await asyncio.sleep(2)
            await asyncio.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("shutting down")
