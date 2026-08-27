"""
onchain_enrich.py - Faza 2 enrichment worker for the on-chain intelligence module.

Polls `onchain_events` for documents with `enriched: false`, runs GoPlus security
+ rugpull and Etherscan deployer lookup, computes threat/early scores + a combined
human verdict, and writes `enrichment`, `scores`, `recommendation`, `enriched: true`.

NEW standalone process. Reuses existing helpers from server.py (no duplication):
    get_goplus_security, get_goplus_rugpull, lookup_etherscan_contract_creator
Those are sync (requests) -> run via asyncio.to_thread so the loop never blocks.

Does NOT touch scanner.py / judge.py / enricher.py / classification.

Scoring is threshold-based (fast, free, auditable). Each score carries reasons[].
THREAT  starts at 0, adds penalties (higher = more dangerous).
EARLY   starts at 50, adds bonuses, and a clean-threat bonus -> a high threat
        score drags early down automatically (don't confuse underground with alpha).

v1 scope: NO funding-cluster graph yet (needs extra deployer tx queries) - that is
a separate step. Deployer lineage here is derived from our own collection (free).

Run with the SAME venv as the backend:
    /srv/data/pump_radar/backend/.venv/bin/python onchain_enrich.py

Env:
    ONCHAIN_MONGO_URL   (default mongodb://localhost:27017)
"""
import asyncio
import logging
import os
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from server import (  # reuse existing helpers
    get_goplus_security,
    lookup_etherscan_contract_creator,
    normalize_solana_goplus_safety,
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("onchain_enrich")

MONGO_URL = os.environ.get("ONCHAIN_MONGO_URL", "mongodb://localhost:27017")
BATCH = 10
CONCURRENCY = 3
POLL_SECONDS = 15

# chain -> GoPlus platform string (GOPLUS_CHAIN_MAP keys in server.py)
GOPLUS_PLATFORM = {"eth": "ethereum", "bsc": "binance-smart-chain", "solana": "solana"}
# chain -> Etherscan key (server.py ETHERSCAN_API_BY_CHAIN; bsc not yet present)
ETHERSCAN_CHAIN = {"eth": "eth", "bsc": "bsc"}

RUG_LABELS = ("scam_likely", "confirmed_rug")


def _f(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _is1(v):
    return str(v) == "1"


def _now():
    return datetime.now(timezone.utc)


_STABLE_NATIVE_ADDR = {
    "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT eth
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC eth
    "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI eth
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
    "0x55d398326f99059ff775485246999027b3197955",  # USDT bsc
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC bsc
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD bsc
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
    "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c",  # BTCB bsc
}
_STABLE_NATIVE_SYM = {"USDT","USDC","DAI","BUSD","WBNB","WETH","BTCB","WSOL","USDC.E","TUSD","FDUSD"}

def _is_stable_or_native(token_addr, symbol):
    if token_addr and token_addr.lower() in _STABLE_NATIVE_ADDR:
        return True
    if symbol and symbol.upper() in _STABLE_NATIVE_SYM:
        return True
    return False


def score(security_data, has_sec, age_days, prior_rugs):
    """Return (scores_dict, recommendation_dict)."""
    sd = security_data or {}
    honeypot = _is1(sd.get("is_honeypot"))
    buy_tax = _f(sd.get("buy_tax")) * 100
    sell_tax = _f(sd.get("sell_tax")) * 100
    max_tax = max(buy_tax, sell_tax)
    open_source = sd.get("is_open_source")  # "1" / "0" / None
    cannot_sell = _is1(sd.get("cannot_sell_all"))
    cooldown = _is1(sd.get("trading_cooldown"))
    blacklist = _is1(sd.get("is_blacklisted"))

    if not has_sec:
        scores = {
            "early": {"score": 50, "label": "early_weak",
                      "reasons": ["GoPlus indisponibil (token prea nou?)"], "computed_at": _now()},
            "threat": {"score": 0, "label": "unknown",
                       "reasons": ["fara date de securitate"], "computed_at": _now()},
        }
        rec = {"verdict": "NO_DATA", "icon": "white", "summary": "date insuficiente, recheck mai tarziu"}
        return scores, rec

    # --- THREAT (penalties from 0) ---
    threat = 0
    treasons = []
    if honeypot:
        threat += 60
        treasons.append("honeypot: nu poti vinde")
    if max_tax > 30:
        threat += 45
        treasons.append(f"taxa {max_tax:.0f}%")
    elif max_tax > 10:
        threat += 25
        treasons.append(f"taxa {max_tax:.0f}%")
    if cannot_sell or cooldown or blacklist:
        threat += 30
        treasons.append("restrictii de vanzare")
    if open_source == "0":
        threat += 20
        treasons.append("contract neverificat")
    if age_days is not None and age_days < 1:
        threat += 10
        treasons.append("deployer creat azi")
    if prior_rugs > 0:
        threat += min(prior_rugs * 30, 60)
        treasons.append(f"deployer cu {prior_rugs} rug-uri anterioare")
    threat = min(threat, 100)

    if honeypot or threat >= 60:
        tlabel = "scam_likely"
    elif threat >= 30:
        tlabel = "suspicious"
    else:
        tlabel = "clean"

    # --- EARLY (bonuses from 50, hard-capped by threat) ---
    early = 50
    ereasons = []
    if open_source == "1" and not honeypot:
        early += 20
        ereasons.append("contract verificat, non-honeypot")
    if max_tax < 5:
        early += 20
        ereasons.append("taxe foarte mici")
    elif max_tax < 10:
        early += 10
        ereasons.append("taxe rezonabile")
    # un threat mare trage early-ul in jos automat
    early = min(early, 100 - threat)
    early = max(0, min(early, 100))
    if early < 50 and not ereasons:
        ereasons.append("plafonat de risc")

    if early >= 75:
        elabel = "early_strong"
    elif early >= 60:
        elabel = "early_watch"
    else:
        elabel = "early_weak"

    scores = {
        "early": {"score": early, "label": elabel, "reasons": ereasons, "computed_at": _now()},
        "threat": {"score": threat, "label": tlabel, "reasons": treasons, "computed_at": _now()},
    }

    if honeypot:
        rec = {"verdict": "HONEYPOT", "icon": "stop", "summary": "nu poti vinde"}
    elif threat >= 60:
        rec = {"verdict": "AVOID", "icon": "red", "summary": "scam likely"}
    elif early >= 75 and threat < 25:
        rec = {"verdict": "WATCH", "icon": "green", "summary": "potential early"}
    elif early >= 60 and threat < 45:
        rec = {"verdict": "CAUTION", "icon": "yellow", "summary": "interesant dar verifica manual"}
    else:
        rec = {"verdict": "SKIP", "icon": "white", "summary": "nimic special"}

    return scores, rec


def _score_solana(sec):
    """Threat/early score din GoPlus Solana (autoritati + concentrare holderi)."""
    n = normalize_solana_goplus_safety(sec)
    if not n.get("available"):
        return None
    threat = 0; tr = []
    if n.get("mintable"):
        threat += 35; tr.append("mint authority activa")
    if n.get("freezable"):
        threat += 30; tr.append("freeze authority activa")
    if n.get("closable") or n.get("balance_mutable_authority"):
        threat += 25; tr.append("control pe balante/cont")
    if n.get("metadata_mutable"):
        threat += 10; tr.append("metadata mutabila")
    thp = n.get("top_holder_percent")
    if thp is not None and thp >= 0.30:
        threat += 15; tr.append("concentrare mare top holder")
    hc = n.get("holder_count")
    if hc is not None and hc < 100:
        threat += 10; tr.append("sub 100 holderi")
    threat = min(threat, 100)
    tlabel = "scam_likely" if threat >= 60 else "suspicious" if threat >= 30 else "clean"
    early = 50; er = []
    if n.get("trusted_token"):
        early += 20; er.append("trusted token")
    if not n.get("safety_red_flags"):
        early += 15; er.append("fara red flags")
    early = max(0, min(early, 100 - threat))
    if early < 50 and not er:
        er.append("plafonat de risc")
    elabel = "early_strong" if early >= 75 else "early_watch" if early >= 60 else "early_weak"
    scores = {
        "early": {"score": early, "label": elabel, "reasons": er, "computed_at": _now()},
        "threat": {"score": threat, "label": tlabel, "reasons": tr, "computed_at": _now()},
    }
    if threat >= 60:
        rec = {"verdict": "AVOID", "icon": "red", "summary": "scam likely"}
    elif early >= 75 and threat < 25:
        rec = {"verdict": "WATCH", "icon": "green", "summary": "potential early"}
    elif early >= 60 and threat < 45:
        rec = {"verdict": "CAUTION", "icon": "yellow", "summary": "interesant dar verifica manual"}
    else:
        rec = {"verdict": "SKIP", "icon": "white", "summary": "nimic special"}
    return scores, rec


async def enrich_one(coll, doc):
    chain = doc["chain"]
    token = doc["token_address"]
    platform = GOPLUS_PLATFORM.get(chain)

    sec = (await asyncio.to_thread(get_goplus_security, platform, token)) if platform else {"available": False}
    dep = await asyncio.to_thread(lookup_etherscan_contract_creator, ETHERSCAN_CHAIN.get(chain, chain), token)

    sd = sec.get("data") or {}
    has_sec = bool(sec.get("available")) and bool(sd)

    deployer_addr = dep.get("deployer_wallet") if dep.get("available") else None
    age_days = None
    created_ts = dep.get("created_timestamp")
    if created_ts:
        try:
            created = datetime.fromtimestamp(int(created_ts), tz=timezone.utc)
            age_days = (_now() - created).days
        except (TypeError, ValueError, OSError):
            age_days = None

    prior_deploys = 0
    prior_rugs = 0
    if deployer_addr:
        da = deployer_addr.lower()
        others = await coll.distinct(
            "token_address",
            {"enrichment.deployer.address": da, "token_address": {"$ne": token}},
        )
        prior_deploys = len(others)
        if others:
            rugged = await coll.distinct(
                "token_address",
                {"token_address": {"$in": others}, "scores.threat.label": {"$in": list(RUG_LABELS)}},
            )
            prior_rugs = len(rugged)

    _sol = _score_solana(sec) if chain == "solana" else None
    if _sol is not None:
        scores, rec = _sol
    else:
        scores, rec = score(sd, has_sec, age_days, prior_rugs)
    if _is_stable_or_native(token, sd.get("token_symbol")):
        scores["early"] = {"score": 0, "label": "not_early", "reasons": ["stablecoin/native, not an early play"], "computed_at": _now()}
        rec = {"verdict": "SKIP", "icon": "white", "summary": "stablecoin / native"}
    else:
        _resv = None
        _g = doc.get("gecko") or {}
        try:
            _resv = float(_g.get("reserve_usd")) if _g.get("reserve_usd") is not None else None
        except (TypeError, ValueError):
            _resv = None
        _e = scores["early"]["score"]
        _er = scores["early"].get("reasons", [])
        if _resv is None:
            _e -= 20; _er.append("fara date de lichiditate")
        elif _resv < 5000:
            _e -= 30; _er.append(f"lichiditate firava (${_resv:.0f})")
        elif _resv < 20000:
            _e -= 15; _er.append(f"lichiditate redusa (${_resv:.0f})")
        elif _resv >= 50000:
            _er.append(f"lichiditate solida (${_resv:.0f})")
        _e = max(0, min(_e, 100))
        scores["early"]["score"] = _e
        scores["early"]["reasons"] = _er
        scores["early"]["label"] = "early_strong" if _e >= 75 else "early_watch" if _e >= 60 else "early_weak"
        _threat = scores["threat"]["score"]
        if not (rec.get("verdict") in ("HONEYPOT","AVOID","NO_DATA")):
            if _e >= 75 and _threat < 25:
                rec = {"verdict": "WATCH", "icon": "green", "summary": "potential early"}
            elif _e >= 60 and _threat < 45:
                rec = {"verdict": "CAUTION", "icon": "yellow", "summary": "interesant dar verifica manual"}
            else:
                rec = {"verdict": "SKIP", "icon": "white", "summary": "nimic special"}

    enrichment = {
        "deployer": {
            "address": deployer_addr.lower() if deployer_addr else None,
            "age_days": age_days,
            "prior_deploys": prior_deploys,
            "prior_rugs": prior_rugs,
            "funded_by": None,
        },
        "funding_cluster": {"root_wallet": None, "cluster_size": 0, "cluster_ids": []},
        "security": {
            "is_honeypot": _is1(sd.get("is_honeypot")),
            "buy_tax": round(_f(sd.get("buy_tax")) * 100, 2),
            "sell_tax": round(_f(sd.get("sell_tax")) * 100, 2),
            "lp_locked_pct": None,
            "is_open_source": sd.get("is_open_source"),
        },
        "liquidity": {"initial_usd": None, "current_usd": None},
        "goplus_available": has_sec,
        "token": {
            "symbol": sd.get("token_symbol") or None,
            "name": sd.get("token_name") or None,
        },
        "base_token": doc.get("base_token"),
    }

    await coll.update_one(
        {"chain": chain, "tx_hash": doc["tx_hash"], "log_index": doc["log_index"]},
        {"$set": {
            "enriched": True,
            "enriched_at": _now(),
            "enrichment": enrichment,
            "scores": scores,
            "recommendation": rec,
        }},
    )
    log.info("[%s] %s -> %s (early=%d threat=%d)",
             chain, token, rec["verdict"], scores["early"]["score"], scores["threat"]["score"])


async def process(coll, doc, sem):
    async with sem:
        try:
            await enrich_one(coll, doc)
        except Exception as e:
            log.warning("enrich failed for %s: %s", doc.get("token_address"), e)


async def main():
    client = AsyncIOMotorClient(MONGO_URL)
    coll = client["pumpradar"]["onchain_events"]
    sem = asyncio.Semaphore(CONCURRENCY)
    log.info("onchain_enrich starting | batch=%d concurrency=%d poll=%ds",
             BATCH, CONCURRENCY, POLL_SECONDS)
    while True:
        batch = await coll.find({"enriched": False}).sort("block_time", -1).limit(BATCH).to_list(BATCH)
        if not batch:
            await asyncio.sleep(POLL_SECONDS)
            continue
        await asyncio.gather(*(process(coll, d, sem) for d in batch))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("shutting down")
