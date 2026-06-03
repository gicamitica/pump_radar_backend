from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


class EngineLogger:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)

    async def log(self, event: str, payload: Dict[str, Any]) -> None:
        record = {
            "event": event,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "payload": payload,
        }
        await asyncio.to_thread(self._append_record, record)

    async def log_run(self, payload: Dict[str, Any]) -> None:
        await self.log("pump_engine_run", payload)

    def _append_record(self, record: Dict[str, Any]) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.db_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, default=str, ensure_ascii=True))
            handle.write("\n")
