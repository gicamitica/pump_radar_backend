"""
bonk_observer.py - Standalone observation logger for BONK.
Runs isolated from main backend. Writes to MongoDB collection 'observation_alpha'.
Do NOT import into server.py. Run via cron:
  0 * * * * cd /srv/data/pump_radar/backend && set -a; source .env; set +a; .venv/bin/python bonk_observer.py >> /var/log/bonk_observer.log 2>&1
"""
import os
import sys
import time
import json
import requests
from datetime import datetime, timezone
from pymongo import MongoClient

ALPHA_ADDR = "2sCUCJdVkmyXp4dT8sFaA9LKgSMK4yDPi9zLHiwXpump"
API_BASE = "http://127.0.0.1:8020"
MONGO_URL = os.environ.get("MONGO_URL")
DB_NAME = os.environ.get("DB_NAME", "pumpradar")

def _safe_get(url, timeout=180):
    try:
        r = requests.get(url, timeout=timeout)
        return r.json()
    except Exception as e:
        return {"error": str(e)[:200]}

def get_price_gecko():
    try:
        r = requests.get(
            f"https://api.geckoterminal.com/api/v2/networks/solana/tokens/{ALPHA_ADDR}",
            timeout=15,
        )
        d = r.json()
        price = d.get("data", {}).get("attributes", {}).get("price_usd")
        return float(price) if price else None
    except Exception:
        return None

def get_signals_v2_entry():
    """Cauta BONK in ultimul snapshot signals-v2."""
    try:
        r = requests.get(f"{API_BASE}/api/crypto/signals-v2", timeout=30)
        d = r.json().get("data", {})
        for cat in ("pump_signals", "watch_signals", "early_signals", "dex_signals", "risk_signals", "dump_signals"):
            for s in d.get(cat, []):
                if s.get("symbol", "").upper() == "ALPHA":
                    return {
                        "category": cat,
                        "confidence": s.get("confidence"),
                        "whale_score": s.get("whale_score"),
                        "whale_accumulation": s.get("whale_accumulation"),
                        "whale_dump_risk": s.get("whale_dump_risk"),
                        "manipulation_probability": s.get("manipulation_probability"),
                        "price_change_h1": s.get("price_change_h1"),
                        "price_change_h24": s.get("price_change_h24"),
                    }
        return {"category": "not_in_scan"}
    except Exception as e:
        return {"error": str(e)[:200]}

def main():
    if not MONGO_URL:
        print("ERROR: MONGO_URL not set", file=sys.stderr)
        sys.exit(1)
    client = MongoClient(MONGO_URL)
    db = client[DB_NAME]
    col = db.observation_alpha

    now_ts = time.time()
    now_iso = datetime.now(timezone.utc).isoformat()

    price = get_price_gecko()
    whale_data = _safe_get(f"{API_BASE}/api/crypto/whale-movements/solana/{ALPHA_ADDR}")
    market_data = _safe_get(f"{API_BASE}/api/crypto/dune/market-signals/solana/{ALPHA_ADDR}")
    enricher = get_signals_v2_entry()

    wd = whale_data.get("data", {}) if isinstance(whale_data, dict) else {}
    md = market_data.get("data", {}) if isinstance(market_data, dict) else {}
    md_wash = md.get("wash_trading", {}) if isinstance(md, dict) else {}

    doc = {
        "timestamp": now_ts,
        "timestamp_iso": now_iso,
        "price_usd": price,
        # Dune whale-movements
        "dune_available": wd.get("available"),
        "dune_error": wd.get("error"),
        "dune_rows_scanned": wd.get("rows_scanned"),
        "dune_events_count": len(wd.get("events", [])) if wd.get("events") else 0,
        "dune_net_pressure_usd": (wd.get("summary") or {}).get("net_pressure_usd"),
        "dune_whales_moving_out": (wd.get("summary") or {}).get("whales_selling_or_moving_out"),
        "dune_whales_withdrawing": (wd.get("summary") or {}).get("whales_withdrawing"),
        "dune_haiku_risk": (wd.get("haiku_verdict") or {}).get("risk"),
        "dune_haiku_reasoning": (wd.get("haiku_verdict") or {}).get("reasoning"),
        # Dune market-signals
        "market_available": md.get("available"),
        "market_buyer_volume_usd": md.get("buyer_volume_usd"),
        "market_seller_volume_usd": md.get("seller_volume_usd"),
        "market_wash_ratio_pct": md_wash.get("wash_ratio_pct"),
        "market_wash_risk": md_wash.get("wash_risk"),
        "market_wash_wallets": md_wash.get("distinct_wash_wallets"),
        # Enricher snapshot (signals-v2 - hourly scan)
        "enricher_category": enricher.get("category"),
        "enricher_confidence": enricher.get("confidence"),
        "enricher_whale_score": enricher.get("whale_score"),
        "enricher_whale_accumulation": enricher.get("whale_accumulation"),
        "enricher_whale_dump_risk": enricher.get("whale_dump_risk"),
        "enricher_manipulation_probability": enricher.get("manipulation_probability"),
        "enricher_price_change_h1": enricher.get("price_change_h1"),
        "enricher_price_change_h24": enricher.get("price_change_h24"),
    }

    col.insert_one(doc)
    print(f"[{now_iso}] logged: price=${price} · dune_events={doc['dune_events_count']} · enricher_cat={enricher.get('category')}")

if __name__ == "__main__":
    main()
