"""Backend read-only pentru Launch Sniper Dashboard, Capitolul 8."""

from __future__ import annotations

import os
import subprocess
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

VERSION = "launch-sniper-dashboard-chapter-8"
DATABASE_NAME = "solbot_launch_sniper"
CAPITAL_ID = "chapter7_paper_capital"

SERVICES = (
    ("watcher", "Launch Watcher", "solbot@launch_watcher.service"),
    (
        "snapshots",
        "Snapshot Worker",
        "solbot@launch_snapshot_worker.service",
    ),
    ("guard", "Security Guard", "solbot@launch_guard_worker.service"),
    ("flow", "Organic Flow", "solbot@launch_flow_worker.service"),
    ("score", "Launch Score", "solbot@launch_score_worker.service"),
    (
        "paper",
        "PAPER Executor",
        "solbot@launch_paper_executor.service",
    ),
)

WORKER_STATE_IDS = {
    "snapshots": "chapter3_snapshot_worker",
    "guard": "chapter4_guard_worker",
    "flow": "chapter5_flow_worker",
    "score": "chapter6_score_worker",
    "paper": "chapter7_paper_executor",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _serialize(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _serialize(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_serialize(item) for item in value]
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    if value is None or isinstance(
        value,
        (str, int, float, bool),
    ):
        return value
    return str(value)


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _systemctl_probe(service: str) -> str:
    try:
        result = subprocess.run(
            ["systemctl", "is-active", service],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
        status = (result.stdout or "").strip().lower()
        if status in {
            "active",
            "activating",
            "deactivating",
            "failed",
            "inactive",
        }:
            return status
        return "unknown"
    except Exception:
        return "unknown"


def _service_rows(
    worker_state: Any,
    probe: Callable[[str], str],
    now: datetime,
) -> list[dict]:
    rows = []
    for key, label, service in SERVICES:
        state_id = WORKER_STATE_IDS.get(key)
        state = (
            worker_state.find_one({"_id": state_id})
            if state_id
            else None
        ) or {}
        heartbeat = state.get("heartbeat_at")
        heartbeat_age = None
        if isinstance(heartbeat, datetime):
            if heartbeat.tzinfo is None:
                heartbeat = heartbeat.replace(tzinfo=timezone.utc)
            heartbeat_age = max(
                0,
                int((now - heartbeat).total_seconds()),
            )
        status = probe(service)
        rows.append(
            {
                "key": key,
                "label": label,
                "service": service,
                "status": status,
                "running": status == "active",
                "heartbeat_at": heartbeat,
                "heartbeat_age_seconds": heartbeat_age,
                "processed": int(state.get("processed") or 0),
                "errors": int(state.get("errors") or 0),
            }
        )
    return rows


def _decision_counts(
    decisions: Any,
    stage: str,
    since: datetime,
) -> dict:
    rows = list(
        decisions.find(
            {
                "stage": stage,
                "decided_at": {"$gte": since},
            },
            {
                "verdict": 1,
                "candidate": 1,
            },
        )
    )
    verdicts = Counter(
        str(row.get("verdict") or "unknown")
        for row in rows
    )
    return {
        "total": len(rows),
        "allow": verdicts.get("allow", 0),
        "review": verdicts.get("review", 0),
        "reject": verdicts.get("reject", 0),
        "candidate": sum(
            1 for row in rows if row.get("candidate") is True
        ),
    }


def _paper_summary(
    capital: dict,
    trades: Any,
    queue: Any,
) -> dict:
    open_rows = list(
        trades.find({"status": "open"})
        .sort("opened_at", -1)
        .limit(5)
    )
    recent_rows = list(
        trades.find({})
        .sort(
            [
                ("closed_at", -1),
                ("opened_at", -1),
            ]
        )
        .limit(20)
    )
    queue_rows = list(
        queue.find(
            {},
            {"status": 1},
        )
    )
    queue_status = Counter(
        str(row.get("status") or "unknown")
        for row in queue_rows
    )
    closed = int(capital.get("closed_count") or 0)
    wins = int(capital.get("wins") or 0)
    losses = int(capital.get("losses") or 0)
    win_rate = wins / closed * 100 if closed else 0.0
    trading = _number(capital.get("trading_capital_usd"), 1000.0)
    exposure = _number(capital.get("open_exposure_usd"))
    return {
        "capital": {
            "base_capital_usd": _number(
                capital.get("base_capital_usd"),
                1000.0,
            ),
            "trading_capital_usd": trading,
            "available_usd": max(0.0, trading - exposure),
            "open_exposure_usd": exposure,
            "realized_pnl_usd": _number(
                capital.get("realized_pnl_usd")
            ),
            "daily_pnl_usd": _number(
                capital.get("daily_pnl_usd")
            ),
            "closed_count": closed,
            "wins": wins,
            "losses": losses,
            "win_rate_pct": round(win_rate, 1),
            "blocked": bool(capital.get("blocked")),
            "blocked_reason": capital.get("blocked_reason"),
            "updated_at": capital.get("updated_at"),
        },
        "open_positions": open_rows,
        "recent_trades": recent_rows,
        "queue": dict(sorted(queue_status.items())),
    }


def build_dashboard_from_db(
    db: Any,
    *,
    now: datetime | None = None,
    service_probe: Callable[[str], str] = _systemctl_probe,
) -> dict:
    """Construiește payloadul fără a modifica MongoDB."""
    current = now or utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    hour_ago = current - timedelta(hours=1)

    launches = db["launch_sniper_launches"]
    snapshots = db["launch_sniper_snapshots"]
    decisions = db["launch_sniper_decisions"]
    queue = db["launch_sniper_paper_queue"]
    trades = db["launch_sniper_paper_trades"]
    capital_state = db["launch_sniper_capital_state"]
    worker_state = db["launch_sniper_worker_state"]

    guard = _decision_counts(
        decisions,
        "chapter4_guard",
        hour_ago,
    )
    flow = _decision_counts(
        decisions,
        "chapter5_organic_flow",
        hour_ago,
    )
    score = _decision_counts(
        decisions,
        "chapter6_launch_score",
        hour_ago,
    )
    launch_count = launches.count_documents(
        {"observed_at": {"$gte": hour_ago}}
    )
    snapshot_count = snapshots.count_documents(
        {"captured_at": {"$gte": hour_ago}}
    )
    paper_opened_1h = trades.count_documents(
        {"opened_at": {"$gte": hour_ago}}
    )

    latest_launch = launches.find_one(
        {},
        sort=[("observed_at", -1)],
    )
    recent_scores = list(
        decisions.find(
            {"stage": "chapter6_launch_score"},
            {
                "_id": 1,
                "symbol": 1,
                "mint": 1,
                "decided_at": 1,
                "verdict": 1,
                "risk_score": 1,
                "opportunity_score": 1,
                "candidate": 1,
            },
        )
        .sort("decided_at", -1)
        .limit(20)
    )
    capital = capital_state.find_one({"_id": CAPITAL_ID}) or {
        "_id": CAPITAL_ID,
        "base_capital_usd": 1000.0,
        "trading_capital_usd": 1000.0,
        "open_exposure_usd": 0.0,
        "realized_pnl_usd": 0.0,
        "daily_pnl_usd": 0.0,
        "closed_count": 0,
        "wins": 0,
        "losses": 0,
        "blocked": False,
    }
    paper = _paper_summary(capital, trades, queue)
    services = _service_rows(
        worker_state,
        service_probe,
        current,
    )

    payload = {
        "version": VERSION,
        "mode": "shadow",
        "paper": True,
        "can_trade": False,
        "can_sign": False,
        "database": DATABASE_NAME,
        "generated_at": current,
        "services": services,
        "services_running": sum(
            1 for row in services if row["running"]
        ),
        "services_total": len(services),
        "pipeline_1h": {
            "launches": launch_count,
            "snapshots": snapshot_count,
            "guarded": guard["total"],
            "guard_allowed": guard["allow"],
            "organic_checked": flow["total"],
            "organic_allowed": flow["allow"],
            "scored": score["total"],
            "candidates": score["candidate"],
            "paper_entries": paper_opened_1h,
        },
        "decisions_1h": {
            "guard": guard,
            "flow": flow,
            "score": score,
        },
        "latest_launch": latest_launch,
        "recent_scores": recent_scores,
        **paper,
    }
    return _serialize(payload)


def build_dashboard() -> dict:
    from pymongo import MongoClient

    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=5_000,
        tz_aware=True,
    )
    db = client[DATABASE_NAME]
    client.admin.command("ping")
    return build_dashboard_from_db(db)


def read_activity(limit: int = 80) -> dict:
    safe_limit = max(1, min(int(limit), 300))
    command = ["journalctl", "--no-pager", "-o", "short-iso"]
    for _, _, service in SERVICES:
        command.extend(["-u", service])
    command.extend(["-n", str(safe_limit)])
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=8,
        )
        lines = [
            line
            for line in (result.stdout or "").splitlines()
            if line.strip()
        ]
        return {
            "lines": lines[-safe_limit:],
            "count": len(lines[-safe_limit:]),
            "generated_at": utc_now().isoformat(),
        }
    except Exception as exc:
        return {
            "lines": [],
            "count": 0,
            "error": str(exc)[:200],
            "generated_at": utc_now().isoformat(),
        }


def self_test() -> dict:
    service_names = [row[2] for row in SERVICES]
    return {
        "version": VERSION,
        "database": DATABASE_NAME,
        "service_count": len(SERVICES),
        "services_unique": len(service_names) == len(set(service_names)),
        "read_only": True,
        "can_trade": False,
        "can_sign": False,
    }


if __name__ == "__main__":
    import json

    print(json.dumps(self_test(), indent=2, ensure_ascii=False))
