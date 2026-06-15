"""
onchain_ingest.py - Faza 1 ingestion service for the on-chain intelligence module.

Subscribes (WSS) to factory `PairCreated` (V2) and `PoolCreated` (V3) events on
ETH + BSC and writes normalized `pair_created` documents into `onchain_events`.

NEW standalone process. Writer only. Does NOT import or touch
scanner.py / judge.py / enricher.py / classification. The enrichment worker and
the API router (onchain_routes.py) are separate.

LP add/lock/remove are NOT handled here - they are emitted by the pair contracts,
not the factory, and require a dynamic per-pair subscription (see TODO at bottom).

Run:
    python onchain_ingest.py

Env (.env on CLOUD):
    ETH_WS_URL=wss://...        # Chainstack ETH websocket
    BSC_WS_URL=wss://...        # Chainstack BSC websocket
    ETH_HTTP_URL=https://...    # for block timestamps (cheaper than WS)
    BSC_HTTP_URL=https://...
    MONGO_URL=mongodb://...     # defaults to the docker mongo
    RPC_PROVIDER=chainstack
"""
import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import httpx
import websockets
from motor.motor_asyncio import AsyncIOMotorClient

# Helius/RPC keys live in the URL - keep httpx from logging full URLs.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("onchain_ingest")

# --- Event signatures (topic0) - verified via keccak256 ---
TOPIC_PAIR_CREATED = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
TOPIC_POOL_CREATED = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"

# --- Per-chain config (all addresses lowercase) ---
CHAINS = {
    "eth": {
        "ws_env": "ETH_WS_URL",
        "http_env": "ETH_HTTP_URL",
        "factories": {
            "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f": "uniswap_v2",
            "0x1f98431c8ad98523631ae4a59f267346ea31f984": "uniswap_v3",
        },
        "base_tokens": {
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
            "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
            "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
        },
    },
    "bsc": {
        "ws_env": "BSC_WS_URL",
        "http_env": "BSC_HTTP_URL",
        "factories": {
            "0xca143ce32fe78f1f7019d7d551a6402fc5350c73": "pancake_v2",
            "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865": "pancake_v3",
        },
        "base_tokens": {
            "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
            "0x55d398326f99059ff775485246999027b3197955",  # USDT
            "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
            "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
        },
    },
}

MONGO_URL = os.environ.get("ONCHAIN_MONGO_URL", "mongodb://localhost:27017")
RECONNECT_BASE = 2      # seconds
RECONNECT_MAX = 60      # seconds


# --- decode helpers ---
def _addr(word: str) -> str:
    h = word[2:] if word.startswith("0x") else word
    return "0x" + h[-40:].lower()


def _words(data: str):
    h = data[2:] if data.startswith("0x") else data
    return [h[i:i + 64] for i in range(0, len(h), 64)]


def _pick_token(token0: str, token1: str, base_tokens) -> tuple:
    """Return (token_address, base_token). The non-base side is the token."""
    t0_base = token0 in base_tokens
    t1_base = token1 in base_tokens
    if t1_base and not t0_base:
        return token0, token1
    if t0_base and not t1_base:
        return token1, token0
    # both base or neither: default token0 as the token, token1 as base
    return token0, token1


def decode_pair_created(chain_cfg, lg) -> dict:
    topics = lg["topics"]
    dex = chain_cfg["factories"].get(lg["address"].lower(), "unknown")
    token0 = _addr(topics[1])
    token1 = _addr(topics[2])
    is_v3 = topics[0].lower() == TOPIC_POOL_CREATED
    w = _words(lg["data"])
    if is_v3:
        fee = int(topics[3], 16) if len(topics) > 3 else None
        pair_addr = _addr(w[1]) if len(w) > 1 else None
        pair_index = None
    else:
        fee = None
        pair_addr = _addr(w[0]) if w else None
        pair_index = int(w[1], 16) if len(w) > 1 else None

    token_addr, base = _pick_token(token0, token1, chain_cfg["base_tokens"])
    return {
        "dex": dex,
        "token_address": token_addr,
        "base_token": base,
        "pair_address": pair_addr,
        "data": {
            "token0": token0,
            "token1": token1,
            "pair_index": pair_index,
            "fee_tier": fee,
            "init_symbol": None,
            "init_decimals": None,
        },
    }


# --- block timestamp cache (per chain, per block) ---
class BlockTimeCache:
    def __init__(self, http_url: str):
        self.http_url = http_url
        self.cache = {}
        self.client = httpx.AsyncClient(timeout=10)

    async def get(self, block_number: int):
        if block_number in self.cache:
            return self.cache[block_number]
        try:
            r = await self.client.post(
                self.http_url,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_getBlockByNumber",
                    "params": [hex(block_number), False],
                },
            )
            ts_hex = r.json().get("result", {}).get("timestamp")
            if ts_hex is None:
                return None
            dt = datetime.fromtimestamp(int(ts_hex, 16), tz=timezone.utc)
            if len(self.cache) > 5000:
                self.cache.clear()
            self.cache[block_number] = dt
            return dt
        except Exception as e:
            log.warning("block time fetch failed (%s): %s", block_number, e)
            return None

    async def close(self):
        await self.client.aclose()


# --- mongo writer ---
async def write_event(coll, doc: dict) -> bool:
    """Dedup upsert: only inserts on first sight, never overwrites enrichment."""
    res = await coll.update_one(
        {"chain": doc["chain"], "tx_hash": doc["tx_hash"], "log_index": doc["log_index"]},
        {"$setOnInsert": doc},
        upsert=True,
    )
    return res.upserted_id is not None


# --- per-chain subscription loop ---
async def run_chain(chain: str, coll):
    cfg = CHAINS[chain]
    ws_url = os.environ.get(cfg["ws_env"])
    http_url = os.environ.get(cfg["http_env"])
    if not ws_url or not http_url:
        log.error("[%s] missing %s / %s in env - skipping", chain, cfg["ws_env"], cfg["http_env"])
        return

    btc = BlockTimeCache(http_url)
    factories = list(cfg["factories"].keys())
    sub_params = [
        "logs",
        {"address": factories, "topics": [[TOPIC_PAIR_CREATED, TOPIC_POOL_CREATED]]},
    ]
    backoff = RECONNECT_BASE

    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": sub_params}))
                ack = json.loads(await ws.recv())
                if "result" not in ack:
                    log.error("[%s] subscribe failed: %s", chain, ack)
                    raise RuntimeError("subscribe failed")
                log.info("[%s] subscribed (sub=%s) factories=%d", chain, ack["result"], len(factories))
                backoff = RECONNECT_BASE  # reset after a clean connect

                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("method") != "eth_subscription":
                        continue
                    lg = msg["params"]["result"]
                    try:
                        await handle_log(chain, cfg, lg, coll, btc)
                    except Exception as e:
                        log.warning("[%s] log handling error: %s", chain, e)
        except Exception as e:
            log.warning("[%s] connection lost: %s - reconnect in %ds", chain, e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX)


async def handle_log(chain, cfg, lg, coll, btc: "BlockTimeCache"):
    block_number = int(lg["blockNumber"], 16)
    block_time = await btc.get(block_number)
    decoded = decode_pair_created(cfg, lg)
    doc = {
        "event_type": "pair_created",
        "chain": chain,
        "block_number": block_number,
        "block_time": block_time,
        "tx_hash": lg["transactionHash"].lower(),
        "log_index": int(lg["logIndex"], 16),
        "factory": lg["address"].lower(),
        "ingested_at": datetime.now(timezone.utc),
        "source": "ws",
        "enriched": False,
        "enrichment": None,
        "scores": {"early": None, "threat": None},
    }
    doc.update(decoded)
    inserted = await write_event(coll, doc)
    if inserted:
        log.info("[%s] new pair %s on %s token=%s", chain, doc["pair_address"], doc["dex"], doc["token_address"])


async def main():
    client = AsyncIOMotorClient(MONGO_URL)
    coll = client["pumpradar"]["onchain_events"]
    log.info("onchain_ingest starting | provider=%s | chains=%s",
             os.environ.get("RPC_PROVIDER", "?"), ",".join(CHAINS.keys()))
    await asyncio.gather(*(run_chain(c, coll) for c in CHAINS))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("shutting down")

# TODO Faza 2 - LP events (lp_add / lp_lock / lp_remove):
#   Maintain a dynamic set of pair_address values discovered above. Open a second
#   logs subscription with address=[those pairs], topics=[[Mint, Burn]]:
#     Mint 0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f
#     Burn 0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496
#   For lp_lock, watch the locker contracts (Unicrypt / Team Finance / PinkLock)
#   or check LP token balances held by those lockers during enrichment.
