"""
PumpRadar Snapshot - Pas 6
Salveaza semnalele in MongoDB si expune trigger pentru scan.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from scanner import collect_all_candidates
from enricher import enrich_all_candidates, fetch_geckoterminal_trending
from judge import judge_candidates

logger = logging.getLogger(__name__)


async def run_full_scan(db) -> Dict:
    started_at = datetime.now(timezone.utc)
    logger.info("=== PumpRadar New Scan START ===")

    try:
        logger.info("Pas 1+2: Colectare surse...")
        candidates = await collect_all_candidates(db)
        logger.info(f"Candidati din surse: {len(candidates)}")

        logger.info("Pas 3: GeckoTerminal trending pools...")
        import httpx
        async with httpx.AsyncClient(follow_redirects=True) as client:
            trending = await fetch_geckoterminal_trending(client)

        logger.info("Pas 3+4: Enrichment...")
        enriched = await enrich_all_candidates(candidates, trending)
        logger.info(f"Candidati enriched: {len(enriched)}")

        if not enriched:
            logger.warning("Niciun candidat enriched - scan oprit")
            return {"success": False, "reason": "no_enriched_candidates"}

        logger.info("Pas 5: AI Judge...")
        signals = await judge_candidates(enriched, db)
        logger.info(f"Semnale finale: {len(signals)}")

        # Deduplicare si filtrare simboluri invalide
        seen_symbols = set()
        deduped = []
        for s in signals:
            sym = s.get("symbol", "")
            if not sym or len(sym) < 2 or len(sym) > 12:
                continue
            if any(c in sym for c in [".", " "]):
                continue
            if sym.isdigit():
                continue
            if sym in seen_symbols:
                continue
            seen_symbols.add(sym)
            deduped.append(s)
        signals = deduped

        # Pas 6: Categorizeaza
        pump_signals = [s for s in signals if s.get("category") == "pump"]
        dump_signals = [s for s in signals if s.get("category") == "dump"]
        risk_signals = [s for s in signals if s.get("category") == "risk"]
        watch_signals = [s for s in signals if s.get("category") == "watch"]
        dex_signals = [s for s in signals if s.get("category") == "dex"]
        early_signals = [s for s in signals if s.get("category") == "early"]

        snapshot = {
            "timestamp": started_at,
            "source_pipeline": "new_architecture_v2",
            "pump_signals": pump_signals,
            "dump_signals": dump_signals,
            "risk_signals": risk_signals,
            "watch_signals": watch_signals,
            "dex_signals": dex_signals,
            "early_signals": early_signals,
            "all_signals": signals,
            "coins_analyzed": len(enriched),
            "candidates_collected": len(candidates),
            "market_summary": (
                f"New architecture scan: {len(pump_signals)} pump, "
                f"{len(dump_signals)} dump, {len(risk_signals)} risk, "
                f"{len(watch_signals)} watch, {len(dex_signals)} dex, "
                f"{len(early_signals)} early signals din {len(enriched)} candidati."
            ),
        }

        await db.signal_snapshots_v2.insert_one(snapshot)
        logger.info("Snapshot salvat in MongoDB (signal_snapshots_v2)")
        try:
            from email_alerts import send_signal_alert_emails
            all_signals = pump_signals + dump_signals + risk_signals + early_signals
            await send_signal_alert_emails(db, all_signals)
        except Exception as e:
            logger.error(f"Signal alert email error: {e}", exc_info=True)

        count = await db.signal_snapshots_v2.count_documents({})
        if count > 48:
            oldest = await db.signal_snapshots_v2.find(
                {}, sort=[("timestamp", 1)]
            ).limit(count - 48).to_list(length=100)
            if oldest:
                await db.signal_snapshots_v2.delete_many(
                    {"_id": {"$in": [d["_id"] for d in oldest]}}
                )

        finished_at = datetime.now(timezone.utc)
        duration = (finished_at - started_at).total_seconds()
        logger.info(f"=== Scan DONE in {duration:.1f}s ===")

        return {
            "success": True,
            "pump_count": len(pump_signals),
            "dump_count": len(dump_signals),
            "risk_count": len(risk_signals),
            "watch_count": len(watch_signals),
            "dex_count": len(dex_signals),
            "early_count": len(early_signals),
            "coins_analyzed": len(enriched),
            "duration_seconds": round(duration, 1),
            "snapshot_at": started_at.isoformat(),
        }

    except Exception as e:
        logger.exception(f"Scan error: {e}")
        return {"success": False, "error": str(e)}


async def get_latest_snapshot(db) -> Optional[Dict]:
    snap = await db.signal_snapshots_v2.find_one({}, sort=[("timestamp", -1)])
    if snap:
        snap.pop("_id", None)
        ts = snap.get("timestamp")
        if hasattr(ts, "isoformat"):
            # Mongo returneaza datetime naive (UTC) - marcam explicit ca UTC
            # ca isoformat sa includa +00:00 si JS sa converteasca corect la ora locala
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            snap["timestamp"] = ts.isoformat()
    return snap
